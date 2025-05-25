# Proyecto Parcial 3: Pipeline de Procesamiento de Noticias con AWS Big Data

Este proyecto implementa un pipeline integral para la extracción, procesamiento, almacenamiento y análisis de noticias de diversos periódicos colombianos (El Tiempo, El Espectador). Utiliza una combinación de servicios de AWS Big Data como Lambda, S3, Glue, EMR y RDS, orquestados mediante flujos de trabajo y despliegue continuo (CI/CD).

---

## 1. Arquitectura General del Pipeline

El pipeline se compone de varias etapas, cada una manejada por servicios específicos de AWS:

1.  **Extracción (Lambda/Glue Job):** Descarga el contenido HTML de las páginas principales de los periódicos.
2.  **Procesamiento (Lambda/Glue Job):** Analiza el HTML usando Beautiful Soup para extraer la categoría, titular y enlace de cada noticia, y los guarda en formato CSV.
3.  **Catalogación (Glue Crawler):** Actualiza el Catálogo de Datos de AWS Glue para que los datos en S3 y RDS sean consultables por Athena y Glue Jobs.
4.  **Almacenamiento Persistente (Glue Job):** Inserta los datos procesados en una base de datos MySQL en AWS RDS, utilizando Job Bookmarks para cargas incrementales.
5.  **Análisis ML (PySpark ML en EMR):** Procesa las noticias usando Pyspark MLlib para vectorizar el texto (TF-IDF) y aplicar un modelo de clasificación, guardando los resultados en S3.
6.  **Orquestación y Despliegue:**
    * **Workflows de Glue:** Encadenan y automatizan las etapas de procesamiento de datos.
    * **Lambda para EMR:** Lanza, ejecuta y termina clusters EMR de forma transitoria para las cargas de trabajo de ML.
    * **GitHub Actions (CI/CD):** Automatiza el despliegue de las funciones Lambda y los scripts de Glue Jobs a AWS.

---

## 2. Configuración de AWS (Prerrequisitos)

Antes de desplegar el código, es **FUNDAMENTAL** configurar los siguientes recursos en tu cuenta de AWS.

### 2.1. Creación de Buckets S3

Crea los siguientes buckets en S3. Asegúrate de que sus nombres sean **globalmente únicos**.

* **`[TU_NOMBRE_DE_USUARIO]-noticias-data`**: Bucket principal para almacenar los datos brutos, procesados y los resultados de ML.
    * Estructura interna:
        * `headlines/raw/` (HTML sin procesar)
        * `headlines/final/` (CSV procesados, con particiones)
        * `ml_results/` (Resultados de ML, e.g., Parquet)
* **`[TU_NOMBRE_DE_USUARIO]-glue-scripts`**: Para almacenar los scripts de AWS Glue Jobs y las bibliotecas/scripts de bootstrap para EMR.
    * Estructura interna:
        * `scripts/`
        * `libs/` (ej: `dependencies.zip` para bibliotecas personalizadas)
        * `bootstrap/` (ej: `install_pyspark_deps.sh`)
* **`[TU_NOMBRE_DE_USUARIO]-emr-logs`**: Para almacenar los logs de los clusters de EMR.

### 2.2. Roles IAM

Necesitarás crear y/o verificar los permisos de los siguientes roles IAM. Es crucial que tengan los permisos correctos para que los servicios interactúen.

* **`LambdaExecutionRole-[NOMBRE_PROYECTO]` (o el que Zappa cree por defecto):**
    * **Permisos:** `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` (en `[TU_NOMBRE_DE_USUARIO]-noticias-data` y `[TU_NOMBRE_DE_USUARIO]-glue-scripts`).
    * `glue:StartCrawler` (para tu crawler S3).
    * `emr:RunJobFlow`, `emr:ListClusters`, `emr:DescribeCluster` (para el orquestador EMR).
    * `iam:PassRole` (para que Lambda pueda pasar los roles de EMR a EMR).
    * Acceso a CloudWatch Logs (`logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`).
* **`AWSGlueServiceRole-[NOMBRE_PROYECTO]` (o uno con similar política):**
    * **Permisos:** `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` (en `[TU_NOMBRE_DE_USUARIO]-noticias-data` y `[TU_NOMBRE_DE_USUARIO]-glue-scripts`).
    * `glue:*` (permisos completos para el servicio Glue).
    * `iam:PassRole` (para que Glue pueda asumir roles).
    * Acceso a CloudWatch Logs.
* **`EMR_DefaultRole` y `EMR_EC2_DefaultRole`:**
    * Estos roles son creados por defecto por EMR. Verifica que `EMR_EC2_DefaultRole` tenga acceso a `[TU_NOMBRE_DE_USUARIO]-noticias-data` y `[TU_NOMBRE_DE_USUARIO]-emr-logs`.

### 2.3. Base de Datos MySQL en AWS RDS

1.  **Crear Instancia RDS:**
    * Ve a la consola de RDS, selecciona "Crear base de datos".
    * Elige **MySQL**.
    * Configura el tamaño (ej: Free tier o `db.t3.small` para pruebas).
    * Define el **identificador de instancia**, **nombre de usuario maestro** y **contraseña maestra**.
    * **Configuración de conectividad:**
        * Asegúrate de que la **VPC Security Group** permita el tráfico entrante en el **Puerto 3306 (MySQL)** desde las subredes y/o grupos de seguridad asociados a tus Glue Jobs y clusters EMR. ¡Este es un punto de falla común!
2.  **Crear Base de Datos y Tabla:**
    * Una vez que la instancia RDS esté disponible, conéctate a ella (ej: usando MySQL Workbench).
    * Crea la base de datos: `CREATE DATABASE IF NOT EXISTS news_db;`
    * Selecciona la base de datos: `USE news_db;`
    * Crea la tabla `headlines` (asegúrate de que los tipos de datos coincidan con los de tus CSV):
        ```sql
        CREATE TABLE IF NOT EXISTS headlines (
            category VARCHAR(255),
            headline TEXT,
            link VARCHAR(2048),
            insert_date DATE DEFAULT (CURDATE())
        );
        ```

### 2.4. Recursos de AWS Glue (Configuración Inicial)

1.  **Conexión Glue a RDS:**
    * En la consola de Glue, ve a **"Conexiones"** -> **"Crear conexión"**.
    * **Nombre:** `mysql-rds-headlines`
    * **Tipo de Conexión:** `JDBC`
    * **URL JDBC:** `jdbc:mysql://[TU_ENDPOINT_RDS]:3306/news_db` (Reemplaza `[TU_ENDPOINT_RDS]`)
    * **Credenciales:** Tu usuario y contraseña maestros de RDS.
    * **Red:** Selecciona la misma VPC, subredes y grupos de seguridad que tu instancia RDS para permitir la conectividad.
    * **¡Prueba la conexión!**
2.  **Crawlers de Glue:**
    * **`headlines_csv_crawler`:**
        * **Ruta de inclusión:** `s3://[TU_NOMBRE_DE_USUARIO]-noticias-data/headlines/final/`
        * **Rol IAM:** Tu `AWSGlueServiceRole`.
        * **Base de datos de salida:** `news_headlines_db` (crea una nueva si no existe).
        * **Programación:** "Ejecutar bajo demanda".
    * **`mysql_headlines_crawler`:**
        * **Tipo de fuente de datos:** `JDBC`
        * **Conexión:** `mysql-rds-headlines`
        * **Ruta de inclusión:** `news_db/headlines`
        * **Rol IAM:** Tu `AWSGlueServiceRole`.
        * **Base de datos de salida:** `news_headlines_db`.
        * **Programación:** "Ejecutar bajo demanda".
        * **¡Ejecuta este crawler una vez manualmente después de crearlo!** Esto creará la tabla `headlines` de RDS en tu Catálogo de Datos de Glue.
3.  **Jobs de Glue (Definición Vacia):**
    * En la consola de Glue, ve a **"Jobs"** -> **"Visual ETL"** -> **"Crear"**.
    * Crea los siguientes Jobs (sin código inicial, solo la definición):
        * `headlines_download_job` (Python Shell)
        * `headlines_process_job` (Python Shell)
        * `headlines_trigger_crawler_job` (Python Shell)
        * `s3_to_rds_headlines_job` (Spark, usarás la interfaz Visual ETL de Glue para configurarlo después de la primera vez que se sincronizan los scripts a S3).
            * Para este job, asegúrate de seleccionar como **Source** la tabla `news_headlines_db.headlines_final` (de S3) y como **Target** la tabla `news_headlines_db.headlines` (de RDS).
            * **IMPORTANTE:** En la configuración del job `s3_to_rds_headlines_job`, bajo "Job details" -> "Job bookmarks", selecciona **"Enable"**. Esto es clave para la carga incremental.

4.  **Workflow de Glue (`DailyHeadlinesProcessingWorkflow`):**
    * En la consola de Glue, ve a **"Workflows"** -> **"Crear flujo de trabajo"**.
    * **Nombre:** `DailyHeadlinesProcessingWorkflow`
    * **Pasos:** Encadena los jobs y triggers de la siguiente manera:
        * **Trigger (Scheduled):** Crea un trigger programado (ej: `cron(0 7 * * ? *)` para 7 AM UTC diaria).
        * **Job 1:** `headlines_download_job`
        * **Job 2 (al éxito de Job 1):** `headlines_process_job`
            * **Parámetros del Job:** `SOURCE_DATE` con valor `#{date -format "yyyy-MM-dd"}` (para pasar la fecha actual).
        * **Job 3 (al éxito de Job 2):** `headlines_trigger_crawler_job`
        * **Job 4 (al éxito de Job 3):** `s3_to_rds_headlines_job`

---

## 3. Código del Proyecto

Ahora, vamos con el código que debes implementar o verificar en tu repositorio.

### 3.1. `src/environment.yml`

```yaml
# src/environment.yml
name: parcial3_env
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.9
  - pip
  - requests
  - boto3
  - beautifulsoup4
  - lxml # Beautiful Soup a veces necesita un parser como lxml o html5lib
  - pyspark # Para desarrollo local de scripts de Glue/EMR Spark
  - pandas # Útil si haces manipulación de datos más compleja
  # Dependencias para Testing
  - pytest
  - moto # Para simular AWS
  - requests-mock # Para simular peticiones HTTP
  - pip:
    - zappa==0.61.1
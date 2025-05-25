---

# Parcial 3 Big Data - Pipeline de Datos de Noticias

Este proyecto implementa un pipeline integral para la recolección, procesamiento, almacenamiento y análisis de titulares de noticias de sitios web colombianos (`eltiempo.com` y `elespectador.com`). Utiliza una arquitectura sin servidor y gestionada en **AWS**, orquestada y desplegada automáticamente con **GitHub Actions**.

## 🚀 Características Principales

* **Extracción de Datos:** Funciones Lambda programadas para descargar titulares de sitios web.
* **Procesamiento y Transformación:** Funciones Lambda y/o Jobs de AWS Glue para parsear HTML, extraer datos relevantes, limpiarlos y almacenarlos en S3 en formato CSV particionado.
* **Catálogo de Datos:** AWS Glue Crawler para catalogar los datos procesados en AWS Glue Data Catalog, haciendo que estén disponibles para análisis con Athena, Spark, etc.
* **Análisis de Machine Learning:** Un Job de Spark en AWS EMR para realizar un análisis de clasificación de los titulares.
* **Automatización de Despliegue (CI/CD):**
    * **Zappa:** Despliegue de funciones Lambda (Python) a AWS API Gateway y Lambda.
    * **GitHub Actions:** Orquestación de todo el pipeline de CI/CD para el despliegue automático de Lambdas y scripts de Glue/EMR a S3.
* **Gestión Segura de Credenciales:** Uso de archivos `.env` para desarrollo local y **GitHub Secrets** para el despliegue en entornos de CI/CD, garantizando que la información sensible nunca se exponga en el código.

---

## 🛠️ Arquitectura y Tecnologías

El proyecto se basa en las siguientes tecnologías:

* **Lenguaje:** Python 3.9
* **Infraestructura como Código (IaC):**
    * **AWS Lambda:** Funciones de extracción, procesamiento y orquestación.
    * **Amazon S3:** Almacenamiento de datos crudos, procesados, scripts de Glue/EMR y paquetes de despliegue de Zappa.
    * **AWS Glue:** Jobs para ETL de datos, y Glue Crawler para catalogación.
    * **Amazon EMR:** Clusters transitorios de Spark para Machine Learning.
    * **Amazon RDS (MySQL):** Opcional, si se usa una base de datos para los resultados finales o metadatos.
    * **AWS IAM:** Gestión de roles y permisos.
* **Librerías Python:** `boto3`, `requests`, `beautifulsoup4`, `pyspark`, `scikit-learn`, `pandas`, `python-dotenv`, `zappa`, `pytest`, `moto`.
* **CI/CD:**
    * **Zappa:** Herramienta para desplegar aplicaciones Python WSGI (Lambdas) a AWS.
    * **GitHub Actions:** Flujos de trabajo para automatizar el despliegue.

---

## 🚀 Configuración y Despliegue

Sigue estos pasos para configurar y desplegar el proyecto.

### 1. Prerrequisitos

Asegúrate de tener instalado lo siguiente:

* **AWS CLI:** Configurado con credenciales que tengan los permisos necesarios para crear y gestionar los recursos de AWS (IAM, S3, Lambda, Glue, EMR, RDS).
* **Python 3.9**
* **Conda/Miniconda:** Para gestionar el entorno de Python.
* **Git**

### 2. Clonar el Repositorio

```bash
git clone https://github.com/devlepa/Parcial3BigData.git
cd Parcial3BigData
```

### 3. Configuración del Entorno Local (`.env`)

Para desarrollo y pruebas locales, utilizaremos un archivo `.env` para las variables de entorno. Este archivo **NO debe subirse a Git**.

1.  Crea un archivo llamado `.env` en la **raíz de tu proyecto** (`Parcial3BigData/.env`).
2.  Añade `.env` a tu archivo `.gitignore` para asegurarte de que no se suba al repositorio.

    ```bash
    echo ".env" >> .gitignore
    ```

3.  Copia el siguiente contenido en tu archivo `.env` y **reemplaza todos los valores `[TU_NOMBRE_DE_USUARIO]` y `xxxxxxxxxxxxxxxxx` con tus datos reales y únicos**.

    ```dotenv
    # .env - Variables de entorno para el proyecto Parcial3BigData
    # ¡IMPORTANTE! Este archivo NO debe ser subido a Git.
    # Asegúrate de que .env esté en tu .gitignore.

    # --- Configuración General de AWS ---
    AWS_REGION=us-east-1 # Tu región de AWS (ej: us-east-1, sa-east-1).

    # --- Nombres de Recursos S3 (Reemplaza [TU_NOMBRE_DE_USUARIO] con tu prefijo único y real) ---
    S3_DATA_BUCKET=[TU_NOMBRE_DE_USUARIO]-noticias-data
    S3_GLUE_SCRIPTS_BUCKET=[TU_NOMBRE_DE_USUARIO]-glue-scripts
    S3_EMR_LOGS_BUCKET=[TU_NOMBRE_DE_USUARIO]-emr-logs
    S3_ZAPPA_DEPLOY_BUCKET=[TU_NOMBRE_DE_USUARIO]-zappa-deployment-bucket

    # --- Nombres de Recursos de AWS Glue ---
    GLUE_CRAWLER_NAME=headlines_csv_crawler # Nombre de tu Glue Crawler para S3
    GLUE_RDS_CONNECTION_NAME=mysql-rds-headlines # Nombre de tu conexión JDBC a RDS en Glue

    # --- Configuración de AWS EMR ---
    EMR_CLUSTER_NAME=NewsMLCluster
    EMR_RELEASE_LABEL=emr-6.15.0 # Asegúrate de que esta versión de EMR sea compatible con Spark
    EC2_SUBNET_ID=subnet-xxxxxxxxxxxxxxxxx # ID de una subred en tu VPC donde quieres que EMR y Glue operen (ej: subnet-0xxxxxxxxxxxxxxxxx)
    EC2_KEY_PAIR=your-ec2-key-pair-name # Opcional: Nombre de tu par de llaves EC2 para acceso SSH a EMR (si lo tienes)
    EMR_SERVICE_ROLE=EMR_DefaultRole # El rol de servicio IAM para EMR (ej: EMR_DefaultRole, debe existir en tu cuenta)
    EC2_INSTANCE_PROFILE=EMR_EC2_DefaultRole # El perfil de instancia IAM para las EC2 de EMR (ej: EMR_EC2_DefaultRole, debe existir)

    # --- Rutas de Scripts en S3 (basadas en los nombres de buckets anteriores) ---
    S3_SCRIPT_LOCATION=s3://${S3_GLUE_SCRIPTS_BUCKET}/scripts/run_ml_pipeline.py
    BOOTSTRAP_ACTION_PATH=s3://${S3_GLUE_SCRIPTS_BUCKET}/bootstrap/install_pyspark_deps.sh

    # --- Credenciales de AWS para pruebas LOCALES (Mantén comentadas para seguridad si no las necesitas localmente) ---
    # AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
    # AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
    ```

### 4. Crear Recursos de AWS (Manual o con CloudFormation/Terraform)

Antes del despliegue, debes asegurarte de que los siguientes recursos existan en tu cuenta de AWS. Los nombres deben coincidir exactamente con los que definiste en tu `.env` y que usarás en GitHub Secrets:

* **Buckets S3:**
    * `[TU_NOMBRE_DE_USUARIO]-noticias-data`: Para datos crudos, procesados y resultados de ML.
    * `[TU_NOMBRE_DE_USUARIO]-glue-scripts`: Para almacenar scripts de Glue y EMR, y scripts de bootstrap.
    * `[TU_NOMBRE_DE_USUARIO]-emr-logs`: Para logs de EMR.
    * `[TU_NOMBRE_DE_USUARIO]-zappa-deployment-bucket`: (Este es creado por Zappa, pero puede ser útil tenerlo en mente para permisos).
* **Roles IAM:**
    * Un rol para **AWS Lambda** con permisos para S3 (lectura/escritura), CloudWatch Logs y permisos para invocar Glue/EMR.
    * Un rol para **AWS Glue** con permisos para S3 (lectura/escritura en los buckets de datos y scripts), CloudWatch Logs y permisos para acceder a Glue Data Catalog y la conexión RDS.
    * Un rol para **AWS EMR (EMR_DefaultRole)** y un perfil de instancia EC2 para EMR (**EMR_EC2_DefaultRole**) con permisos para S3 (lectura/escritura en buckets de datos, scripts y logs), y CloudWatch Logs.
* **Conexión AWS Glue a RDS:** Una conexión JDBC en Glue para tu instancia MySQL de RDS, si vas a utilizarla.
* **AWS Glue Crawler:** Un crawler llamado `headlines_csv_crawler` (o el nombre que definas) que apunte a la ruta `s3://[TU_NOMBRE_DE_USUARIO]-noticias-data/headlines/final/` para catalogar tus datos CSV.
* **Subred EC2:** Identifica una `EC2_SUBNET_ID` en tu VPC donde puedan operar EMR y Glue.

### 5. Configuración de GitHub Secrets

**¡Este paso es CRÍTICO para el CI/CD!** No subas tus credenciales o valores sensibles al repositorio.

1.  Ve a tu repositorio en GitHub.
2.  Haz clic en **Settings** (Configuración).
3.  En la barra lateral izquierda, navega a **Secrets and variables** > **Actions**.
4.  Haz clic en **New repository secret** (Nuevo secreto de repositorio) para cada una de las siguientes variables. Los **nombres** deben coincidir exactamente y los **valores** deben ser los que usaste en tu `.env`.

    * `AWS_ACCESS_KEY_ID`
    * `AWS_SECRET_ACCESS_KEY`
    * `AWS_REGION`
    * `S3_DATA_BUCKET`
    * `S3_GLUE_SCRIPTS_BUCKET`
    * `S3_EMR_LOGS_BUCKET`
    * `S3_ZAPPA_DEPLOY_BUCKET`
    * `GLUE_CRAWLER_NAME`
    * `GLUE_RDS_CONNECTION_NAME`
    * `EMR_CLUSTER_NAME`
    * `EMR_RELEASE_LABEL`
    * `EC2_SUBNET_ID`
    * `EC2_KEY_PAIR` (Si no usas, puedes dejarlo vacío o con un dummy value si no tienes forma de no incluirlo en el zappa_settings.json, pero es mejor que coincida con lo que tu rol EMR espera)
    * `EMR_SERVICE_ROLE`
    * `EC2_INSTANCE_PROFILE`
    * `S3_SCRIPT_LOCATION`
    * `BOOTSTRAP_ACTION_PATH`

### 6. Instalar Dependencias Locales

Crea y activa el entorno Conda para instalar las dependencias:

```bash
conda env create -f src/environment.yml
conda activate parcial3_env
```

### 7. Ejecutar Tests Unitarios (Opcional pero Recomendado)

Antes de desplegar, puedes ejecutar los tests unitarios para verificar la lógica de tus funciones.

```bash
pytest tests/unit/
```

### 8. Despliegue Automático con GitHub Actions

El proyecto está configurado para despliegue continuo usando GitHub Actions.

* **`deploy-lambdas.yml`**: Se activa con `push` en `main` si hay cambios en `src/lambda_functions/`, `src/zappa_settings.json` o `src/environment.yml`.
    * Configura las credenciales de AWS.
    * **Reemplaza dinámicamente** los placeholders en `src/zappa_settings.json` con los `GitHub Secrets`.
    * Ejecuta tests unitarios.
    * Despliega/Actualiza las funciones Lambda con Zappa.
* **`deploy-glue-jobs.yml`**: Se activa con `push` en `main` si hay cambios en `src/glue_jobs/`.
    * Configura las credenciales de AWS.
    * Sincroniza los scripts de Glue y EMR (`src/glue_jobs/`) con tu bucket `S3_GLUE_SCRIPTS_BUCKET` bajo el prefijo `scripts/`.

Simplemente haz un `git push` a la rama `main` después de configurar tus secretos y verificar tu código, y los workflows de GitHub Actions se encargarán del despliegue.

```bash
git add .
git commit -m "Initial commit with CI/CD setup"
git push origin main
```

Monitorea la pestaña "Actions" en tu repositorio de GitHub para ver el progreso del despliegue.

---

## 🧐 Puntos Críticos y Notas

* **Selectores HTML:** La extracción de datos en `src/lambda_functions/process_headlines.py` y `src/glue_jobs/process_headlines_glue_job.py` depende **CRÍTICALMENTE** de los selectores CSS/HTML usados con `BeautifulSoup`. Los proporcionados son **EJEMPLOS GENÉRICOS**. Si los sitios web (`eltiempo.com`, `elespectador.com`) cambian su estructura HTML, deberás actualizar estos selectores para que la extracción funcione correctamente. **¡Inspecciona el HTML actual de las páginas web para obtener los selectores correctos!**
* **Permisos IAM:** Asegúrate de que los roles IAM que tus Lambdas, Glue Jobs y EMR asumen tengan los **permisos adecuados** para leer y escribir en los buckets S3, invocar servicios (Glue, EMR), y acceder a Glue Data Catalog.
* **Costos de AWS:** Este proyecto utiliza servicios de AWS que incurren en costos (Lambda, S3, Glue, EMR, CloudWatch). Monitorea tu uso para evitar sorpresas en la factura.
* **Programación:** Las funciones Lambda están programadas con eventos `cron` en `zappa_settings.json`. Ajusta los horarios según tus necesidades.

---

## 🤝 Contribución

¡Siéntete libre de contribuir a este proyecto! Puedes abrir _issues_ o _pull requests_ si encuentras errores o tienes mejoras.

---
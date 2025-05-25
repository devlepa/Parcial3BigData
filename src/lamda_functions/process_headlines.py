import boto3
import csv
from io import StringIO
from urllib.parse import unquote_plus
from bs4 import BeautifulSoup
import re # Import regex for category extraction

# Initialize S3 client
s3 = boto3.client('s3')

# Define your S3 bucket name (same as the previous one)
# It's good practice to get this from environment variables or Zappa settings
S3_BUCKET_NAME = os.environ.get('S3_BUCKET', 'your-unique-bucket-name-here') # Make sure this matches!

def extract_news_data(html_content, source_newspaper):
    """
    Extracts category, headline, and link from HTML content using Beautiful Soup.
    This function will need to be adapted based on the actual HTML structure
    of El Tiempo and El Espectador.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []

    if source_newspaper == 'el_tiempo':
        # Example for El Tiempo - This will likely need adjustments!
        # Inspect El Tiempo's homepage HTML (e.g., using browser developer tools)
        # Look for common patterns for news articles (e.g., <div> with class="article-card")
        articles = soup.find_all('article', class_='article-card') # Common pattern
        for article in articles:
            headline_tag = article.find('h2', class_='title') # Or h3, a specific class
            link_tag = article.find('a', class_='link') # Or directly in the headline tag
            category_tag = article.find('p', class_='category') # Look for a category tag

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'

            # Simple category extraction: often in URL or specific text
            category = category_tag.get_text(strip=True) if category_tag else 'General' # Default

            # Try to infer category from link if not found explicitly
            if category == 'General' and link and 'eltiempo.com/' in link:
                match = re.search(r'eltiempo\.com/noticias/([^/]+)/', link)
                if match:
                    category = match.group(1).replace('-', ' ').title()

            news_items.append({
                'category': category,
                'headline': headline,
                'link': link
            })
    elif source_newspaper == 'el_espectador':
        # Example for El Espectador - This will also need adjustments!
        # Inspect El Espectador's homepage HTML
        articles = soup.find_all('div', class_='Card-Body') # Another common pattern
        for article in articles:
            headline_tag = article.find('h2', class_='Card-Title')
            link_tag = article.find('a', class_='Card-Link')
            category_tag = article.find('span', class_='Card-Category')

            headline = headline_tag.get_text(strip=True) if headline_tag else 'N/A'
            link = link_tag['href'] if link_tag and 'href' in link_tag.attrs else 'N/A'
            category = category_tag.get_text(strip=True) if category_tag else 'General'

            if category == 'General' and link and 'elespectador.com/' in link:
                 match = re.search(r'elespectador\.com/noticias/([^/]+)/', link)
                 if match:
                    category = match.group(1).replace('-', ' ').title()

            news_items.append({
                'category': category,
                'headline': headline,
                'link': link
            })
    # Add more elif blocks for other newspapers like publimetro

    return news_items

def process_s3_html_event(event, context):
    """
    Lambda function triggered by S3 PUT events in the 'raw' folder.
    Processes the HTML, extracts data, and saves to CSV in 'final' folder.
    """
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key']) # Decode URL-encoded key

        # Expected key format: headlines/raw/newspaper-contenido-yyyy-mm-dd.html
        print(f"Processing S3 object: {key} from bucket: {bucket_name}")

        try:
            # 1. Download the HTML content
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            html_content = obj['Body'].read().decode('utf-8')

            # 2. Determine the newspaper and date from the key
            # Example key: headlines/raw/el_tiempo-contenido-2023-10-27.html
            parts = key.split('/')
            filename = parts[-1] # e.g., el_tiempo-contenido-2023-10-27.html
            name_parts = filename.split('-contenido-')
            if len(name_parts) < 2:
                print(f"Skipping malformed key: {key}")
                continue

            newspaper_name = name_parts[0] # e.g., el_tiempo
            date_str = name_parts[1].replace('.html', '') # e.g., 2023-10-27
            year, month, day = date_str.split('-')

            # 3. Extract data using Beautiful Soup
            extracted_data = extract_news_data(html_content, newspaper_name)

            if not extracted_data:
                print(f"No news data extracted from {key}. Skipping CSV creation.")
                continue

            # 4. Prepare CSV content in memory
            csv_buffer = StringIO()
            fieldnames = ['category', 'headline', 'link']
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            for item in extracted_data:
                writer.writerow(item)

            csv_content = csv_buffer.getvalue()

            # 5. Define target S3 key for CSV
            # s3://bucket/headlines/final/periodico=xxx/year=xxx/month=xxx/day=xxx/data.csv
            csv_s3_key = (f'headlines/final/periodico={newspaper_name}/year={year}/'
                          f'month={month}/day={day}/{newspaper_name}_headlines.csv')

            # 6. Upload CSV to S3
            print(f"Uploading CSV to s3://{bucket_name}/{csv_s3_key}...")
            s3.put_object(Bucket=bucket_name, Key=csv_s3_key, Body=csv_content, ContentType='text/csv')
            print(f"Successfully processed {key} and uploaded CSV.")

        except Exception as e:
            print(f"Error processing {key}: {e}")
            # You might want to log this error to a more persistent store or trigger an alert

    return {
        'statusCode': 200,
        'body': 'HTML processing and CSV upload completed.'
    }
import duckdb
from datetime import datetime
from bs4 import BeautifulSoup
from minio import Minio
from io import BytesIO
import pandas as pd
from urllib.parse import urlparse
import re


# MinIO client setup
minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

# DuckDB connection
conn = duckdb.connect()


def extract_text_from_html(html_content):
    #Extract clean text from HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    # Get text and clean it
    text = soup.get_text()
    
    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)
    
    return text


def extract_metadata_from_html(html_content, filename):
    #Extract metadata from HTML content
    soup = BeautifulSoup(html_content, 'html.parser')
    
    metadata = {
        'title': '',
        'description': '',
        'url': '',
        'publish_date': None
    }
    
    # Extract title
    title_tag = soup.find('title')
    if title_tag:
        metadata['title'] = title_tag.get_text().strip()
    
    # Extract description from meta tags
    desc_tag = soup.find('meta', attrs={'name': 'description'}) or \
               soup.find('meta', attrs={'property': 'og:description'})
    if desc_tag:
        metadata['description'] = desc_tag.get('content', '').strip()
    
    # Try to extract URL from canonical link or og:url
    url_tag = soup.find('link', attrs={'rel': 'canonical'}) or \
              soup.find('meta', attrs={'property': 'og:url'})
    if url_tag:
        metadata['url'] = url_tag.get('href') or url_tag.get('content', '')
    
    # Try to extract publish date
    date_tag = soup.find('meta', attrs={'property': 'article:published_time'}) or \
               soup.find('meta', attrs={'name': 'publish_date'}) or \
               soup.find('time', attrs={'datetime': True})
    if date_tag:
        date_str = date_tag.get('content') or date_tag.get('datetime')
        if date_str:
            metadata['publish_date'] = date_str
    
    return metadata


def process_raw_to_bronze():
    #Transform raw HTML files to bronze structured data
    print("Starting Bronze transformation...")
    
    # List all objects in raw bucket
    try:
        raw_objects = minio_client.list_objects("raw", recursive=True)
        bronze_records = []
        
        for obj in raw_objects:
            if obj.object_name.endswith('.html'):
                print(f"Processing: {obj.object_name}")
                
                try:
                    # Read HTML from MinIO
                    response = minio_client.get_object("raw", obj.object_name)
                    html_content = response.read().decode('utf-8')
                    response.close()
                    response.release_conn()
                    
                    # Extract text and metadata
                    clean_text = extract_text_from_html(html_content)
                    metadata = extract_metadata_from_html(html_content, obj.object_name)
                    
                    # Create bronze record
                    bronze_record = {
                        'file_id': obj.object_name.replace('.html', ''),
                        'raw_filename': obj.object_name,
                        'title': metadata['title'],
                        'description': metadata['description'],
                        'url': metadata['url'],
                        'publish_date': metadata['publish_date'],
                        'extracted_text': clean_text,
                        'text_length': len(clean_text),
                        'word_count': len(clean_text.split()) if clean_text else 0,
                        'processed_at': datetime.now().isoformat(),
                        'processing_stage': 'bronze'
                    }
                    
                    bronze_records.append(bronze_record)
                    
                except Exception as e:
                    print(f"Error processing {obj.object_name}: {e}")
                    continue
        
        if bronze_records:
            # Save to MinIO as Parquet
            save_to_minio_bronze(bronze_records)
            
            # Register in DuckDB catalog
            register_in_duckdb(bronze_records)
            
            print(f"Bronze transformation completed! Processed {len(bronze_records)} records")
        else:
            print("No records to process")
            
    except Exception as e:
        print(f"Error accessing raw bucket: {e}")


def save_to_minio_bronze(records):
    #Save bronze records to MinIO bronze bucket as Parquet
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = "bronze_articles.parquet"
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Convert DataFrame to Parquet bytes
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    parquet_bytes = buffer.getvalue()
    buffer.close()
    
    # Upload to MinIO
    minio_client.put_object(
        bucket_name="bronze",
        object_name=filename,
        data=BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream"
    )
    
    print(f"Saved to MinIO bronze bucket: {filename} ({len(parquet_bytes)} bytes)")


def register_in_duckdb(records):
    #Register bronze data in DuckDB catalog
    try:
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Create/replace bronze table in DuckDB
        conn.execute("DROP TABLE IF EXISTS bronze_articles")
        conn.register('bronze_articles_df', df)
        conn.execute("""
            CREATE TABLE bronze_articles AS 
            SELECT * FROM bronze_articles_df
        """)
        
        # Show summary
        result = conn.execute("SELECT COUNT(*) as total, AVG(word_count) as avg_words FROM bronze_articles").fetchone()
        print(f"DuckDB: {result[0]} records, avg {result[1]:.0f} words per article")
        
    except Exception as e:
        print(f"Error registering in DuckDB: {e}")




if __name__ == "__main__":
    process_raw_to_bronze()
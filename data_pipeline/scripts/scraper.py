import requests # for making HTTP requests to fetch web pages
from bs4 import BeautifulSoup # for parsing HTML content and extracting clean text
import feedparser # for parsing XML based rss feeds
from urllib.parse import urlparse # for breaking down urls
from datetime import datetime # for generating timestamps for filenames
from minio import Minio
from io import BytesIO


# MinIO client setup
minio_client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

# BBC feed rss url
rss_feed = "http://feeds.bbci.co.uk/news/technology/rss.xml"


def ensure_minio_buckets():
    """Create MinIO buckets if they don't exist"""
    buckets = ["raw", "bronze", "silver", "gold"]
    for bucket in buckets:
        try:
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket already exists: {bucket}")
        except Exception as e:
            print(f"Error with bucket {bucket}: {e}")


def fetch_rss_link(rss_url=rss_feed, limit=50):
    """Function to fetch article titles and urls from rss feed"""
    feed = feedparser.parse(rss_url) # for parsing the rss xml to return a structured feed object
    entries = feed.entries[:limit] # get upto 50 items (title+link) from the feed
    return [(entry.title, entry.link) for entry in entries] # return a list of (title, url) tuples


def sanitize_filename(name):
    """Replace non-alphanumeric characters with underscores"""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)


def fetch_and_save_articles(title, url):
    """Fetch article content and save to MinIO raw bucket"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Generate filename
        filename_base = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{sanitize_filename(title)}"
        html_filename = f"{filename_base}.html"

        # Convert HTML to bytes for MinIO storage
        html_data = response.text.encode('utf-8')
        
        # Upload to MinIO raw bucket
        minio_client.put_object(
            bucket_name="raw",
            object_name=html_filename,
            data=BytesIO(html_data),
            length=len(html_data),
            content_type="text/html"
        )
        print(f"Uploaded to MinIO raw bucket: {html_filename}")

    except Exception as e:
        print(f"Failed to fetch {url} | {e}")


def run_scraper():
    """Main function to run the scraping process"""
    print("Setting up MinIO buckets...")
    ensure_minio_buckets()
    
    print("Fetching RSS feed...")
    articles = fetch_rss_link()
    
    print(f"Found {len(articles)} articles. Starting scraping...")
    for i, (title, url) in enumerate(articles, 1):
        print(f"Processing {i}/{len(articles)}: {title[:50]}...")
        fetch_and_save_articles(title, url)
    
    print("Scraping completed!")


if __name__ == "__main__":
    run_scraper()
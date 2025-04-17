#!/usr/bin/env python3
"""
Ingestion script for Financial News Summarizer & Q&A project.
Fetches RSS/Atom feeds, deduplicates entries, and uploads raw JSON to S3.
"""
import os
import json
import hashlib
import logging
from datetime import datetime

import feedparser
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# ------ Configuration via environment variables ------
# Comma-separated list of RSS/Atom feed URLs
FEEDS = os.environ.get(
    'FEEDS',
    'https://feeds.reuters.com/reuters/businessNews,https://www.ft.com/?format=rss'
).split(',')
# S3 bucket for raw articles
S3_BUCKET = os.environ.get('S3_BUCKET', 'finance-news-raw')
# AWS region for the S3 bucket
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
# -----------------------------------------------------

# Initialize S3 client
s3 = boto3.client('s3', region_name=AWS_REGION)


def generate_s3_key(entry) -> str:
    """
    Generate a unique S3 key for each RSS entry based on its ID or link and the publish date.
    Format: YYYY/MM/DD/<sha1>.json
    """
    # Use entry ID or link as the unique source
    uid_source = entry.get('id') or entry.get('link', '')
    uid = hashlib.sha1(uid_source.encode('utf-8')).hexdigest()

    # Determine the publication date
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        dt = datetime(*entry.published_parsed[:6])
    else:
        dt = datetime.utcnow()
    date_path = dt.strftime('%Y/%m/%d')

    return f"{date_path}/{uid}.json"


def upload_entry(entry):
    """
    Check if the entry exists in S3 (using GetObject), and if not, upload it as JSON.
    """
    key = generate_s3_key(entry)
    try:
        # s3.head_object maps to GetObject permission
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        logging.debug(f"Entry already exists, skipping: {key}")
        return
    except ClientError as e:
        err_code = e.response['Error']['Code']
        if err_code in ('404', 'NoSuchKey', 'NotFound'):
            # Object does not exist, proceed to upload
            pass
        else:
            # Other errors (e.g., permissions), log and skip
            logging.error(f"Error checking existence for {key}: {e}")
            return

    # Build the JSON record
    record = {
        'title':      entry.get('title', ''),
        'link':       entry.get('link', ''),
        'summary':    entry.get('summary', ''),
        'published':  entry.get('published', ''),
    }
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(record).encode('utf-8'),
            ContentType='application/json'
        )
        logging.info(f"Uploaded: s3://{S3_BUCKET}/{key}")
    except ClientError as e:
        logging.error(f"Failed to upload {key}: {e}")


def process_feed(url: str):
    """
    Fetch and process all entries from a single RSS/Atom feed URL.
    """
    logging.info(f"Fetching feed: {url}")
    feed = feedparser.parse(url)
    if feed.bozo:
        logging.error(f"Failed to parse feed {url}: {feed.bozo_exception}")
        return
    for entry in feed.entries:
        try:
            upload_entry(entry)
        except Exception as e:
            logging.error(f"Unexpected error processing entry: {e}")


def main():
    for feed_url in FEEDS:
        process_feed(feed_url)


if __name__ == '__main__':
    main()

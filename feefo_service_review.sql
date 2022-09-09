import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
#import numpy as np
import json
import requests
import datetime
from google.cloud import bigquery
client = bigquery.Client()

def _get_merchant_ratings(merchant_identifiers, since_period='24_hours'):
    """Return a list of dictionaries containing the product and service ratings for each merchant.
    
    Args:
        merchant_identifiers (list): List of Feefo merchant identifiers
        since_period (string, optional): Time since review (24_hours, week, month, year, all)
    
    Returns:
        Python list containing dictionaries of merchant ratings and addresses.
    """
    
    results = []

    endpoint = "https://api.feefo.com/api/10/reviews/summary/all"
    
    for merchant_identifier in merchant_identifiers:
        response = requests.get(endpoint + "?merchant_identifier=" + merchant_identifier + "&since_period=" + since_period)    
        results.append(json.loads(response.text))

    return results


results = _get_merchant_ratings(['ct-shirts-uk'])
results


def _count_results(results):
    """Return the number of results in a paginated result set."""
    
    if results:
        return results.get('summary').get('meta').get('count')
    else:
        return
def _count_pages(results):
    """Return the number of pages in a paginated result set."""
    
    if results:
        return results.get('summary').get('meta').get('pages')
    else:
        return
    
    
def _get_merchant_reviews(merchant_identifier, since_period='24_hours'):
    """Return the reviews for a given Feefo merchant. 
    
    Args:
        merchant_identifier (string): Feefo merchant identifier
        since_period (string, optional): Time since review (24_hours, week, month, year, all)
    
    Return:
        Python list of dictionaries of product reviews
    """
    
    response = requests.get("https://api.feefo.com/api/10/reviews/all?page_size=100&merchant_identifier="+merchant_identifier+"&since_period="+since_period)
    results = json.loads(response.text)
    
    total_results = _count_results(results)
    total_pages = _count_pages(results)
    reviews = results['reviews']
    
    i = 2
    while(i <= total_pages):
        response = requests.get("https://api.feefo.com/api/10/reviews/all?page_size=100&merchant_identifier="+merchant_identifier+"&since_period="+since_period)
        results = json.loads(response.text)
        reviews.extend(results['reviews'])
        i += 1
    
    return reviews


def get_merchant_reviews(merchant_identifier, since_period='24_hours'):
    """Return the reviews for a given Feefo merchant in a Pandas dataframe. 
    
    Args:
        merchant_identifier (string): Feefo merchant identifier
        since_period (string, optional): Time since review (24_hours, week, month, year, all)

    Return:
        Pandas dataframe of product reviews
    """
    
    reviews = _get_merchant_reviews(merchant_identifier, since_period)
    
    df = pd.DataFrame(columns=['merchant_identifier', 'customer', 'created_at', 'review', 'service_rating'])
    for review in reviews:

        row = {
            'merchant_identifier': review.get('merchant').get('identifier'),
            'customer': review.get('customer', {}).get('display_name', {}),
            'created_at': review.get('service', {}).get('created_at', {}),
            'review': review.get('service', {}).get('review', {}),
            'service_rating': review.get('service', {}).get('rating', {}).get('rating', {}),           
        }

        df = df.append(row, ignore_index=True)
    
    df['service_rating'] = df['service_rating'].astype(int) #CHECK POINT 1
    df['avg'] = df['service_rating'].astype(int)
    #df['helpful_votes'] = df['helpful_votes'].astype(int)
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['type'] = "Service Reviews"
    
    df['id'] = df["customer"].map(str) + df['created_at'].map(str) + df['service_rating'].map(str) + df['review'].map(str)
    df = df.drop_duplicates(subset='id', keep='last')

    return df

def post_data(request, args):
  df = get_merchant_reviews('ct-shirts-uk', since_period='24_hours')
  df['customer'] = df['customer'].astype(str)
  df['review'] = df['review'].astype(str)
  df['id'] = df['id'].astype(str)
  df['service_rating'] = df['service_rating'].astype(int)
  
  final = pd.DataFrame(df, columns = ["customer", "created_at", "review", "service_rating", "type", "id"])

  job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("customer", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("created_at", bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField("service_rating", bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField("review", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("type", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
    ],)
  table_id = 'schema.dataset.table'
  job = client.load_table_from_dataframe(final, table_id, job_config = job_config)
  job.result()

  return None

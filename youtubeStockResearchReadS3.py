import os
import json
import decimal
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv

# Load environment variables for configuration (Lambda or local)
DYNAMO_TABLE_NAME = "channel_id_sortbytime_table"
S3_BUCKET_NAME ="transcript-summary"
CACHE_TABLE_NAME = "channel_summary_cache"

role_arn = 'arn:aws:iam::440597413354:role/localRole'

# Only load .env when running locally (not in Lambda)
if os.getenv("AWS_EXECUTION_ENV") is None:
    from dotenv import load_dotenv
    load_dotenv()

def get_boto3_session():
    if 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
        # Inside Lambda: use default creds injected by AWS
        return boto3.Session()
    else:
        # Local dev: assume role using IAM user creds
        sts = boto3.client('sts')
        assumed = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="YoutubeSummarySession"
        )
        creds = assumed['Credentials']
        return boto3.Session(
            aws_access_key_id=creds['AccessKeyId'],
            aws_secret_access_key=creds['SecretAccessKey'],
            aws_session_token=creds['SessionToken']
        )

session = get_boto3_session()

# Create AWS clients/resources
s3 = session.resource('s3')
dynamodb = session.resource('dynamodb', region_name='us-east-2')

summary_table = dynamodb.Table(DYNAMO_TABLE_NAME)
cache_table = dynamodb.Table(CACHE_TABLE_NAME)

# Constants
CACHE_TTL_SECONDS = 300  # 5 minutes


def generate_cache_key(channel_id):
    """Create a deterministic cache key for a channel."""
    return f"{channel_id}_2weeks"

def get_summaries_from_s3(channel_id, items):
    """
    Fetch S3 summary objects for the given list of items from DynamoDB.
    Each item must have a 'published_at' field.
    """
    summaries = []
    for item in items:
        published_at = item["published_at"]
        video_id = item["video_id"]

        # Format S3 object key
        dt = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        date_only = dt.date().isoformat()

        #if fetching prefilter, s3_key = f"{channel_id}/{date_only}/{channel_id}_{published_at}_prefiltered.json"
        s3_key = f"{channel_id}/{date_only}/{channel_id}_{video_id}_{published_at}.json"

        try:
            #This line does not fetch the file's contents yet — it just creates a handle to the object.
            s3_obj = s3.Object(S3_BUCKET_NAME, s3_key)
            
            #    This actually retrieves the contents of the object using get(), which returns a dictionary.
            #    The ["Body"] key gives you a streaming body (like a file-like object).
            #    .read() reads the entire content into memory as bytes.
            #    .decode("utf-8") converts the bytes to a UTF-8 string (since S3 stores data as bytes).
            s3_data = s3_obj.get()["Body"].read().decode("utf-8")

            # json.loads() parses the UTF-8 string into a Python dictionary or list (depending on your JSON structure).
            summaries.append(json.loads(s3_data))
        except ClientError as e:
            print(f"Failed to fetch S3 object {s3_key}: {e}")
    return summaries

#Get error
#Object of type Decimal is not JSON serializable on json.dumps(items)
#Due to DynamoDB returns numbers as Decimal objects (from the decimal module), and Python’s built-in json.dumps() doesn’t know how to serialize them.
def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        return float(obj) if obj % 1 else int(obj)
    return obj


def lambda_handler(event, context):
    if event['headers'].get("x-custom-gateway-secret") != 'sec-value-206':
        return {
            "statusCode": 403,
            "body": json.dumps({"error": "Forbidden API Header"})
        }

    """
    Lambda entry point. Expects event to contain a "channel_id" key.
    Returns summaries for the past 2 weeks from S3, using DynamoDB for index and caching.
    """
    #channel_id = json.loads(event["body"]).get("channel_id")
    channel_id = event["queryStringParameters"].get("channel_id")
    if not channel_id:
        return {"statusCode": 400, "body": "Missing 'channel_id'"}

    '''
    # Intended to store the result of this function in cache but decided to just pull all the summaries
    # Calling S3 each time for these 2 months worth of stuff is not that bad
    # DynamoDB can only store 400KB per entry
    # If I really needed to cache could have used REDIS

    cache_key = generate_cache_key(channel_id)
    # Check the cache table for a recent summary
    try:
        response = cache_table.get_item(Key={"cache_key": cache_key})
        cached = response.get("Item")
        if cached:
            print("Cache hit")
            return {
                "statusCode": 200,
                "body": cached["data"],
                "cached": True
            }
        else:
            print("Cache miss")
    except ClientError as e:
        print(f"Cache fetch failed: {e}")
    '''
    
    # Cache miss: query DynamoDB for past 2 months
    now = datetime.now(timezone.utc)
    date_range = now - timedelta(days=60)

    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_range_str = date_range.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        response = summary_table.query(
            KeyConditionExpression=Key("channel_id").eq(channel_id) &
                                   Key("published_at").between(date_range_str, now_str)
        )

        #This still handles the case where the DynamoDB query returns no items
        #If the query is successful but no matching items are found, the response will still be a valid dictionary — but "Items" will be an empty list ([]).
        #The .get("Items", []) ensures items is always a list — either the list of matched items or an empty list if nothing was found.
        items = response.get("Items", [])
        if not items:
            print("No dynamoDB channel_id/published_at record found for this channel for the past 2 months.")
    except ClientError as e:
        print(f"DynamoDB query failed: {e}")
        return {"statusCode": 500, "body": "DynamoDB query error"}

    print(f"\nFetched dynamoDB summary metadata for the {channel_id}: {response}\n")
    # Fetch summaries from S3
    #summaries = get_summaries_from_s3(channel_id, items)
    #print('\nprinted sumamries below:\n')
    #print(summaries)
    
    '''
    # Cache the result in DynamoDB with a TTL
    try:
        ttl_timestamp = int(datetime.now(timezone.utc).timestamp()) + CACHE_TTL_SECONDS
        cache_table.put_item(
            Item={
                "cache_key": cache_key,
                "data": json.dumps(summaries, indent=2),
                "ttl": ttl_timestamp  # DynamoDB TTL attribute
            }
        )
        print("Cache updated")
    except ClientError as e:
        print(f"Failed to update cache: {e}")
    '''

    items_clean = convert_decimals(items)

    return {
        "statusCode": 200,
        "body": json.dumps(items_clean)
        #"body": json.dumps(summaries, indent=2),
    }

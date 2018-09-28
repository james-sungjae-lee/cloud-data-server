import boto3
import json

from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('amazon-product-electronics')
    print "Event : ", event
    asin = event["asin"]
    
    response = table.query(
        ProjectionExpression = "asin, category, price, title, imUrl",
        KeyConditionExpression = Key('asin').eq(asin) 
    )
    
    item = response['Items'][0]
    result = "asin : "+str(item["asin"])+" product name : "+str(item["title"])+ " product price : "+str(item["price"])
    return result


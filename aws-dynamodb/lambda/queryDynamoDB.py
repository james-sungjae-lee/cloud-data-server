import boto3
import json
import decimal

from boto3.dynamodb.conditions import Key, Attr

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('apd-electronics-asin')
    print "Event : ", event
    asin = event["asin"]
    
    response = table.query(
        ProjectionExpression= "asin, price, title, imUrl",
        KeyConditionExpression=Key('asin').eq(asin) & Key('price').gt(0) 
    )
    
    item = response['Items'][0]
    result = "asin : "+str(item["asin"])+" product name : "+str(item["title"])+ " product price : "+str(item["price"])
    return result

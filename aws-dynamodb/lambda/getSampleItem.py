import boto3
import json
import decimal

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

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
    asin = event["asin"]
    
    dynamodb = boto3.resource("dynamodb", region_name='us-west-2')
    table = dynamodb.Table('amazon-product-electronics')
    
    try:
        response = table.get_item(
            Key = {
                'asin': asin
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
        return "Failed get item"
    else:
        item = response['Item']
        print("Get Item Successed : ")
        print(json.dumps(item, indent=4, cls=DecimalEncoder))
        return "Successed get item"


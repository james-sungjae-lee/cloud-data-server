import boto3
import json

from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    table = dynamodb.Table('amazon-product-electronics')
    print "Event : ", event
    category = event["category"]
    
    response = table.query(
        IndexName = 'category-price-index',
        ProjectionExpression= "asin, category, price, title, imUrl",
        KeyConditionExpression=Key('category').eq(category) & Key('price').gt(0) 
    )
    
    html = '<html><head><title>Amazon Product Electronics</title></head>'+'<body><h1>Amazon Product Electronics</h1>'
    
    for item in response['Items']:
        html += "<h3>asin : "+str(item["asin"])+"</h3>"
        html += "<h3>product name : "+str(item["title"])+"</h3>"
        html += "<h3>product price : $"+str(item["price"])+"</h3>"
        html += "<img src="+str(item["imUrl"])+"><br>"

    html += '</body></html>'
    
    return html

#!/bin/bash

aws dynamodb put-item --table-name amazon-product-electronics --item file://item2.json
echo "dynamodb put-item item2.json"

aws dynamodb put-item --table-name amazon-product-electronics --item file://item3.json
echo "dynamodb put-item item3.json"

aws dynamodb put-item --table-name amazon-product-electronics --item file://item4.json
echo "dynamodb put-item item4.json"

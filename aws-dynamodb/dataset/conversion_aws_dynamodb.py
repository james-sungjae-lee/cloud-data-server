import json
import gzip
JSON_FILE_READ_PATH="meta_Electronics.json.gz"

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield eval(l)


index = 0
json_array = []
for json_data in parse(JSON_FILE_READ_PATH):
    json_put_request = {}
    json_item = {}
    if index == 25:
        break
    try:
        if json_data["title"] == None :
            pass
        elif json_data["price"] == None :
            pass
        elif json_data["imUrl"] == None :
            pass
        elif json_data["categories"] == None :
            pass
                
        json_item = {
                "asin" : {"S":json_data["asin"]}, 
                "price":{"N":str(float(json_data["price"]))},
                "title":{"S":json_data["title"][0:24]},
                "imUrl":{"S":json_data["imUrl"]}
        }
        json_dict = {"PutRequest" : {"Item": json_item}}
        json_array.append(json_dict)
    except Exception, e:
        pass
    index += 1
result_json = {"amazon-review-dataset-electronics":json_array}

with open('request-100.json', 'w') as outfile:
    json.dump(result_json, outfile, indent = 4)

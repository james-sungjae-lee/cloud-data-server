import json
import gzip
JSON_FILE_READ_PATH="meta_Electronics.json.gz"

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield eval(l)


index = 0
dataset_size = 0
json_array = []
for json_data in parse(JSON_FILE_READ_PATH):
    json_put_request = {}
    json_item = {}
    if dataset_size == 25:
        break
    elif index == 20000:
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
        #print json_data["categories"][1]     
        json_item = {
                "category": {"S": str(json_data["categories"][1][1]).replace("&", "").replace("  "," ")},
                "price":{"N":str(float(json_data["price"]))},
                "title":{"S":json_data["title"]},
                "imUrl":{"S":json_data["imUrl"]}
        }
        json_dict = {"PutRequest" : {"Item": json_item}}
        json_array.append(json_dict)
        print dataset_size
        dataset_size += 1
    except Exception, e:
        pass
    index += 1
result_json = {"amazon-product-electronics":json_array}

with open('request-items.json', 'w') as outfile:
    json.dump(result_json, outfile, indent = 4)
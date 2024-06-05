import requests
# 500 errors are server errors.
	
# use method available to look through all methods of the object.
# print(dir(r))

# r.headers will give you back all the headers that comes back with the response.
import requests
import json
url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
r = requests.get(url, headers = {"ID Nation" : "01000US", 'Population': '316515021'})
try:
    r.status_code
         # returns the http status , if success it will give back 200, 300s are redirets, 400s are client errors
except:
    print("Dammit failed to extract data")

# print(json.dumps(r.text, indent = 3, sort_keys= True))
with open("/Users/admin/Desktop/LearningSparkV2/chapter2/py/src/data/test.json", "w") as f:
    f.write(json.dumps(r.text, indent = 3, sort_keys= True))
    f.close()
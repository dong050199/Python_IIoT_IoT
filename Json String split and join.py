import json

data = dict()

data["tag1"] = 10.12312312
data["tag2"] = 11
data["tag3"] = 12
data["tag4"] = 13
data["tag5"] = 14

data_out = json.dumps(data)


pythonObj = json.loads(data_out)
tag1 = pythonObj["tag1"]
print(tag1)
print(data_out)

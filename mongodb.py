from pymongo import MongoClient
client = MongoClient()
db = client.test
document_1 = {
     "title": "Working With MongoDB in Python",
     "author": "K.sumanth",
     }
test = db.test
result = test.insert_one(document_1)


import re

from pymongo import MongoClient

myclient = MongoClient("mongodb+srv://TwitterDB:twitterextraction@twittercluster.udioj.mongodb.net/test")
db = myclient["ProcessedDB"]
Collection = db["ProcessedTweet"]
rawDb = myclient["RawDB"]
TestCollection = rawDb["TestTweet"]
x = TestCollection.find()
tweetList = []

for i in x:

    newRecord = {}
    if 'text' in i:

        textField = i['text']
        # do operations
        textField = re.sub(r'\s+', ' ', textField)
        textField = re.sub(r"http\S+", '', textField)
        textField = re.sub(r"[^a-zA-Z0-9!@',.\$& ]", '', textField)
        textField = re.sub(r'\\u[A-Za-z0-9]{4}', '', textField)
        textField = re.sub(r'&amp;', '&', textField)
        textField = re.sub(r'\\n', ' ', textField)

        newRecord['text'] = textField
    if 'id' in i:
        newRecord['id'] = i['id']
    if 'source' in i:
        newRecord['source'] = i['source']
    if 'quote_count' in i:
        newRecord['quote_count'] = i['quote_count']
    if 'in_reply_to_screen_name' in i:
        newRecord['in_reply_to_screen_name'] = i['in_reply_to_screen_name']
    if 'in_reply_to_user_id_str' in i:
        newRecord['in_reply_to_user_id_str'] = i['in_reply_to_user_id_str']
    if 'created_at' in i:
        newRecord['created_at'] = i['created_at']

    tweetList.append(newRecord)
Collection.insert_many(tweetList)

print("ProcessedDB contains cleaned tweets where special characters, URLs, emoticons, ampersand, etc are removed")


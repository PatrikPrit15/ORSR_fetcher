from flask import Flask
from flask import jsonify

from pymongo import MongoClient

# pripojenie k db
try:
    print("Pripojuje sa k databáze.")
    client = MongoClient("mongodb://localhost:27017/")
    db = client["firmy_db"]
    collection = db["firmy_collection"]
    print("Databáza bola pripojená.")

except Exception as error:
    print("Databázu sa nepodarilo pripojiť: ", error)



app = Flask(__name__)

# oendpoint detail
@app.route('/detail/<ico>')
def detail(ico):
    ans = []
    for x in collection.find({'ico':int(ico)}, {'_id':0}):
        ans.append(x)

    if (len(ans)==0):
        return 'Not found', 404
    return jsonify(ans[0])

# endpoint list
@app.route('/list')
def list_():
    ans=[]
    for x in collection.find():
        ans.append({'ico' : x['ico'], 'obchodneMeno' : x['obchodneMeno']})

    return jsonify(ans)
    


if __name__ == '__main__': 
   app.run()

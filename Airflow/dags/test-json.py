import json

f = open("companies.json","r")

companies= json.loads(f.read())['data']

print(len(companies))

c=1
for i in companies : 
    print(f"{c:2} : {i['trade_symbol']}")
    c+=1

import json
import csv
import sys

restaurant_business = {}

f = open('Dataset/business.json','r')
fr = open('restaurant_charlotte_business.txt','w')

city="Charlotte"

j = json.JSONDecoder()

for bid_data in f:
    business = j.decode(bid_data)
    if city == business["city"]  and'Restaurants' in business['categories'] :
        restaurant_business[business['business_id']] = True
        fr.write(bid_data)
f.close()
fr.close()

#sys.exit(0)

f_filtered_review = open('restaurant_charlotte_reviews.csv','w')

f = open('Dataset/review.json')
fieldnames = ['review_id','user_id','business_id','stars']
writer = csv.DictWriter(f_filtered_review, fieldnames = fieldnames)

writer.writeheader();
relevant_users = set()

unique = {}

for review in f:
    rv = j.decode(review)
    if rv["business_id"] in restaurant_business:
        key = rv["business_id"]+'_'+rv['user_id']
        if key not in unique:
            unique[key]=True
            writer.writerow({ k : rv [k] for k in fieldnames  })
            relevant_users.add(rv["user_id"])

f.close()
f_filtered_review.close()

f = open("Dataset/user.json")

fuser = open("Restaurant_user.txt",'w');

for line in f:
    user = j.decode(line);
    if user["user_id"] in relevant_users:
        fuser.write(line);

f.close()
fuser.close()

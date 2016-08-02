import json
import sys
import csv

j = json.JSONDecoder()

f = open('Restaurant_user.txt')

fw = open('User.csv','w')

headers = ['user_id','review_count', 'fans', 'average_stars', 'useful']

writer = csv.DictWriter(fw, fieldnames = headers)
writer.writeheader()

for line in f:
    user = j.decode(line)
    newdict = {}
    newdict["user_id"] = user["user_id"]
    newdict["review_count"] = user["review_count"]
    newdict["fans"] = user["fans"]
    newdict["average_stars"] = user ["average_stars"]
    newdict["useful"] = user["votes"]["useful"]
    writer.writerow(newdict);

f.close()
fw.close()
    

    
        

    

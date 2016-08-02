import csv
import json

category_dict = {}
j = json.JSONDecoder()

f = open('Category.csv')

for l in f.readlines()[1:]:
    i,cat = l.strip().split(',',1)
    cat = cat.replace('"','')
    category_dict[cat.strip()] = int(i)


f.close()

print category_dict.keys()

f = open("restaurant_business.txt")
fw = open('business_category_map.csv','w')
    
header = ['map_id','business_id','category_id']

writer = csv.DictWriter(fw,fieldnames = header)

writer.writeheader()

i=1
for l in f:
    business = j.decode(l)
    for category in business["categories"]:
        newdict = {}
        newdict["map_id"] = i
        newdict["category_id"] = category_dict[category]
        newdict["business_id"] = business["business_id"]
        writer.writerow(newdict)
        i=i+1

f.close()


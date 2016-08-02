import csv
import sys
import json

fc = open('Category.csv')

categories = dict()
for x in fc:
    categories[x.strip()]=0;
   
def normalize(s):
    return s.replace(' ','_').replace('/','_').replace('(','').replace(')','').replace(',','_').replace('-','_').strip().lower()

def get_category_dict(cat):
    d = {}
    for k in categories.keys():
        d["cat_%s"%k] = 0;
    for c in cat:
        if normalize(c) in categories:
            d["cat_%s"%(normalize(c))] = 1
    return d

def get_ambience_dict(x):
    d = {
        'romantic' : 0,
        'intimate' : 0,
        'classy' : 0,
        'hipster' : 0,
        'divey' : 0,
        'touristy' : 0,
        'trendy' : 0,
        'upscale' : 0,
        'casual' : 0,
    }
    for k in x.keys():
        d[k] = 1 if x[k] else 0
    return d

def get_good_for_dict(x):
    '''
        "dessert": false, "latenight": false, "lunch": false, "dinner": false, "breakfast": true, "brunch": true},
    '''
    d = {
        'dessert':0,
        'latenight':0,
        'lunch':0,
        'dinner':0,
        'breakfast':0,
        'brunch':0,
    }
    for k in x.keys():
        d[k] = 1 if x[k] else 0
    return d

def get_price_range_dict(x):
    d = {
    'pr_1':0,
    'pr_2':0,
    'pr_3':0,
    'pr_4':0,
    }
    d["pr_%d"%x] = 1
    return d

j = json.JSONDecoder()

f = open('restaurant_charlotte_business.txt')

fw = open('Business.csv','w')

#headers = ['business_id','name', 'city', 'stars', 'review_count','good_for_kids', 'good_for_groups', 'price_range',
#    'alcohol','waiter_service','parking','ambience','take_out','drive_thru','delivery','good_for','open']

headers = ['business_id','good_for_kids', 'good_for_groups',
    'alcohol','waiter_service','parking','take_out','drive_thru','delivery','open']
headers.extend(get_good_for_dict({}).keys())
headers.extend(get_ambience_dict({}).keys())
headers.extend(get_category_dict([]).keys())
headers.extend(get_price_range_dict(2).keys())

writer = csv.DictWriter(fw, fieldnames = headers)
writer.writeheader()

for line in f:
    business = j.decode(line)
    business_attr = business['attributes']
    #print business.keys()
    #print business_attr.keys()
    #print business
    #for category in business["categories"]:
        #categories.add(category)
    newdict = {}
    newdict["business_id"] = business["business_id"]
    #newdict["review_count"] = business["review_count"]
    #newdict["name"] = business["name"].encode("utf-8")
    #newdict["stars"] = business["stars"]
    #newdict["city"] = business["city"].encode('utf-8')
    newdict.update(get_category_dict(business["categories"]))
    if "Good for Kids" in business_attr:
        newdict["good_for_kids"]= 1 if business_attr["Good for Kids"] else 0
    else:
        newdict["good_for_kids"]=0
    if "Good For Groups" in business_attr:
        newdict["good_for_groups"] = 1 if business_attr["Good For Groups"] else 0
    else:
        newdict["good_for_groups"] = 0
    if "Price Range" in business_attr:
        newdict.update(get_price_range_dict(business_attr["Price Range"]))
    else:
        newdict.update(get_price_range_dict(2)) #Default price range

    if "Alcohol" in business_attr:
        newdict["alcohol"] = 0 if business_attr["Alcohol"] == "none" else 1
    else:
        newdict["alcohol"] = 0
    if "Waiter Service" in business_attr:
        newdict["waiter_service"] = 1 if business_attr["Waiter Service"]  else 0
    else:
        newdict["waiter_service"] = 0
    if "Drive-Thru" in business_attr:
        newdict["drive_thru"] = 1 if business_attr["Drive-Thru"]  else 0
    else:
        newdict["drive_thru"] = 0
    newdict["open"] = 1 if business["open"] else 0
    if "Delivery" in business_attr:
        newdict["delivery"]=1 if business_attr["Delivery"]  else 0
    else:
        newdict["delivery"]=0
    if "Take-out" in business_attr:
        newdict["take_out"] = 1 if business_attr["Take-out"] else 0
    else:
        newdict["take_out"] = 0
    if "Parking" in business_attr:
        newdict["parking"] = 1 if True in business_attr["Parking"].values() else 0
    else:
        newdict["parking"] = 0
    if "Ambience" in business_attr:
        newdict.update(get_ambience_dict(business_attr["Ambience"]))
    else:
        newdict.update(get_ambience_dict({}))
    if "Good For" in business_attr:
        newdict.update(get_good_for_dict(business_attr["Good For"]))
    else:
        newdict.update(get_good_for_dict({}))
    writer.writerow(newdict);
    #sys.exit(0)

i=1
'''
f_cat = open('Category.csv','w')
writer_cat = csv.DictWriter(f_cat, fieldnames = ["name"])

writer_cat.writeheader()
for cat in categories:
    newdict = {}
    #newdict["category_id"] = i
    newdict["name"] = cat
    writer_cat.writerow(newdict)
    i=i+1
'''
f.close()
fw.close()

f_final = open('Business_2col.csv','w')
header = ["business_id","attributes"]
writer = csv. DictWriter(f_final,fieldnames = header)
writer.writeheader()

f = open('Business.csv','r')
f.readline()

for x in f:
    d= {}
    d['business_id'],d['attributes'] = x.strip().split(',',1)
    d['attributes'] = d['attributes'].replace(',',' ')
    writer.writerow(d)

f.close()
f_final.close()



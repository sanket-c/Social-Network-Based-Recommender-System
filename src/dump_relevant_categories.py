import json

f = open('restaurant_charlotte_business.txt')
j = json.JSONDecoder()

categories = set()

def normalize(s):
    return s.replace(' ','_').replace('/','_').replace('(','').replace(')','').replace(',','_').replace('-','_').strip().lower()
    

for l in f:
    b = j.decode(l)
    for category in b["categories"]:    
        categories.add(normalize(category))

fc = open("Category.csv",'w')

for category in categories:
    fc.write(category+'\n');

fc.close()

f.close()

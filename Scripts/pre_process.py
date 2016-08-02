import csv
import sys
import json
import os
import pickle


j = json.JSONDecoder()
dataset = "../Dataset/"


def get_business_dict(city):
    '''
        city: city for filtering
        Creates a dictionary of businesses in the specified city
        Compiles a list of categories these businesses have later
        on user as features
    '''
    
    bfile = open(dataset+'business.json','r')
    businesses = {}
    categories = set()
    for l in bfile:
        business = j.decode(l)
        #print business['city'].lower() 
        if business['city'].lower() == city:
            #print business['categories']
            if 'Restaurants' in business['categories']:     
                bid = business['business_id']
                businesses[bid] = business
                for category in business['categories']:
                    categories.add(category)
    bfile.close()            
    return businesses, categories

def filter_relevant_users(b_dict):
    '''
        b_dict : businesses
        Returns a tuple of two dictionaries

        reviews : relevant reviews  filtered by businesses
        users: relevant users
    '''
    rfile = open(dataset+'review.json','r')
    reviews = {}
    users = {}
    for l in rfile:
        review = j.decode(l)
        bid = review['business_id']
        rid = review['review_id']
        uid = review['user_id']
        if bid in b_dict:
            reviews[rid] = review
            if uid not in users:
                users[uid] = 1
            else:
                users[uid] += 1
    users = { u : v for u,v in users.items() if v >= 3  }
    rfile.close()

    return reviews,users
    
def build_graph(users):
    '''
        users: dictionary of users
        build an adjacecy list, edge list, and final dictionary of relevant users
    '''
    users_final = {}
    graph = {}
    edges = {}
    ufile = open(dataset+'user.json')
    for l in ufile:
        user = j.decode(l)
        uid = user['user_id']
        if uid in users:
            users_final[uid] = user
            if not user["friends"]:
                continue
            graph[uid] = set()
            for friend in user["friends"]:
                if friend in users:
                    graph[uid].add(friend)
                    if uid < friend:
                        edges[(uid,friend)] = True 
                    else:
                        edges[(friend,uid)] = True

    ufile.close()
    return users_final,graph,edges
    
def normalize(s):
    '''
        normalize the string 
    '''
    return s.replace(' ','_').replace('/','_').replace('(','').replace(')','').replace(',','_').replace('-','_').strip().lower()

def get_category_dict(categories,cat):
    '''
        Create category dict for CSV writing
    '''
    d = {}
    for k in categories.keys():
        d["cat_%s"%k] = 0;
    for c in cat:
        if normalize(c) in categories:
            d["cat_%s"%(normalize(c))] = 1
    return d

def get_ambience_dict(x):
    '''
        Create Ambience dict for CSV writing
    '''
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
        Create good for dict for CSV writing
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
    '''
        Creat dict for price range for writing to CSV
    '''
    d = {
    'pr_1':0,
    'pr_2':0,
    'pr_3':0,
    'pr_4':0,
    }
    d["pr_%d"%x] = 1
    return d
    
def dump_files_stage(users,graph,categories,edges):
    '''
        Create csv files
        users: User.csv
        graph: graph.pickle
        categories: categories.txt
        edges: edges.txt
    '''
    cat_file_name = '../csvs/categories.txt'
    graph_file_name = '../pickles/graph.pickle'
    edges_file = '../csvs/edges.txt'
    users_file = '../csvs/User.csv' 

    cf = open(cat_file_name,'w')
    for category in categories:
        cf.write(normalize(category)+'\n')
    cf.close()
    
    graph_f = open(graph_file_name,'w')
    pickle.dump(graph,graph_f)
    graph_f.close()

    edge_f = open(edges_file,'w')
    edge_f.write('user,friend\n')
    for edge in edges.keys():
        edge_f.write("%s,%s\n"%edge)
    edge_f.close()

    ufile = open(users_file, 'w')
    headers = ['user_id','average_stars']
    writer = csv.DictWriter( ufile ,fieldnames = headers)
    writer.writeheader()
    for u in users.values():
        writer.writerow({ 'user_id': u['user_id'] , 'average_stars':u['average_stars'] })
    ufile.close()
        
def dump_business_file(businesses):
    ''' 
        Converts the data in  businesses dictionary into csv file
    '''
    business_file = '../csvs/Business.csv'
    business_col_file = '../csvs/Business_col.csv'
    bfile = open(business_col_file,'w')
    
    categories = open('../csvs/categories.txt').read().splitlines()
    categories = {category: True for category in categories}

    headers = ['business_id','good_for_kids', 'good_for_groups',
    'alcohol','waiter_service','parking','take_out','drive_thru','delivery','open']
    headers.extend(get_good_for_dict({}).keys())
    headers.extend(get_ambience_dict({}).keys())
    headers.extend(get_category_dict(categories,[]).keys())
    headers.extend(get_price_range_dict(2).keys())

    writer = csv.DictWriter(bfile, fieldnames = headers)
    writer.writeheader()

    #categories = open('../csvs/categories.txt').read().splitlines()
    #categories = {category: True for category in categories}

    for business in businesses.values():
        #business = j.decode(line)
        business_attr = business['attributes']
        newdict = {}
        newdict["business_id"] = business["business_id"]

        newdict.update(get_category_dict(categories,business["categories"]))
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

    bfile.close()

    f_final = open(business_file,'w')
    header = ["business_id","attributes"]
    writer = csv. DictWriter(f_final,fieldnames = header)
    writer.writeheader()

    f = open(business_col_file,'r')
    f.readline()

    for x in f:
        d= {}
        d['business_id'],d['attributes'] = x.strip().split(',',1)
        d['attributes'] = d['attributes'].replace(',',' ')
        writer.writerow(d)

    f.close()
    f_final.close()

def ubpickle(businesses, users, reviews, edges):
    ''' 
        Converts reviews into Ratings.csv

        Makes use of reviews to create two dictionaries:

        user_businesses : user <=> list of businesses user has rated
        
        rating_dicts : user+"_"+business <=> rating 

        for both test and train data

        Divides reviews into train and test using a R-script split.R
        Reads the csv of train and test and creates edges_train.txt and edges_test.txt

    '''
    ratings = {}
    review_file = '../csvs/Ratings.csv' 
    f = open(review_file,'w')
    headers =  ['review_id','user_id','business_id','stars']
    writer = csv.DictWriter(f,fieldnames = headers)
    writer.writeheader() 
    for r in reviews.values():
        if r['business_id'] in businesses and r['user_id'] in users:
            writer.writerow({u:v for u,v in r.items() if u in headers})
    f.close()
    os.system('Rscript split.R')
    f = open('../csvs/Ratings_train.csv','r')
    f.readline()
    user_businesses_train = {}
    rating_train_dict = {}
    user_businesses_test = {}
    rating_test_dict = {}
    user_train = {}
    for l in f:
        review_id,user_id,business_id,stars = l.strip().split(',')
        if user_id not in user_businesses_train:
            user_businesses_train [user_id] = set()
        user_train [user_id] = True
        user_businesses_train[user_id].add(business_id)
        key = user_id+'_'+business_id 
        rating_train_dict[key] = int(stars)
    f.close()

    fp1 = open('../pickles/user_businesses_train.pickle','w')
    pickle.dump(user_businesses_train,fp1)
    fp1.close()
    
    fp2 = open('../pickles/rating_train_dict.pickle','w')
    pickle.dump(rating_train_dict,fp2)
    fp2.close()
   
    f = open('../csvs/Ratings_test.csv','r')
    f.readline()
    user_businesses_test = {}
    rating_test_dict = {}
    user_test = {}
    for l in f:
        review_id,user_id,business_id,stars = l.strip().split(',')
        if user_id not in user_businesses_test:
            user_businesses_test [user_id] = set()
        user_test[user_id] = True
        user_businesses_test[user_id].add(business_id)
        key = user_id+'_'+business_id
        rating_test_dict[key] = int(stars)
    f.close()

    fp1 = open('../pickles/user_businesses_test.pickle','w')
    pickle.dump(user_businesses_test,fp1)
    fp1.close()

    fp2 = open('../pickles/rating_test_dict.pickle','w')
    pickle.dump(rating_test_dict,fp2)
    fp2.close()
     
    fedge_train = open('../csvs/edges_train.txt','w')
    fedge_test = open('../csvs/edges_test.txt','w')
    
    fedge_train.write('user,friend\n')
    fedge_test.write('user,friend\n')
    for edge in edges.keys():
        if edge[0]  in user_train and edge[1] in user_train:
            fedge_train.write("%s,%s\n"%edge)
        if edge[0]  in user_test and edge[1] in user_test:
            fedge_test.write("%s,%s\n"%edge)    
        

    fedge_train.close()
    fedge_test.close()

def main():
    
    argc = len(sys.argv)
    if argc != 2:
        print "Usage: python pre_process.py <city>"
        return
    city = sys.argv[1]
    businesses,  categories = get_business_dict(city)
    print "Filtered businesses and Extracted Categories"
    reviews, users = filter_relevant_users(businesses)
    print "Extracted reviews and relevant users"
    users, graph, edges  = build_graph(users)
    print "Built the graph and edge list"
    dump_files_stage(users,graph,categories,edges)
    print "Dumped the data into respective files"
    raw_input('Check ../csvs/categories.txt file to remove categories that are not required\n')
    dump_business_file(businesses)
    print "Created business files"
    ubpickle(businesses,users,reviews,edges)
    print "Generated train and test csvs and pickles from Ratings"
    
if __name__ == "__main__":
    main()    
    

    
        
        
     
    



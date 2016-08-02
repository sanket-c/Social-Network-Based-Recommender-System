from pyspark.sql import SQLContext
import pickle
import math
import random
import sys
import numpy as np

#random seeds
random_seed = 15
random.seed(random_seed)

#filenames for pickles and csvs to be loaded
weights_pickle = "pickles/weights.pickle"
priors_pickle = "pickles/priors.pickle"
attributes_prob_pickle = "pickles/attributes_probability.pickle"
correlation_pickle = "pickles/correlation.pickle"
graphs_pickle = "pickles/graph.pickle"
rating_dict_file = 'pickles/rating_test_dict.pickle'
ratings_train_dict_file = 'pickles/rating_train_dict.pickle'
user_businesses_file = 'pickles/user_businesses_test.pickle'
users_file = 'csvs/user_test.csv'
ratings_file = 'csvs/Ratings_test.csv'
business_file = 'csvs/Business.csv'
user_friends_file = 'csvs/edges_test.txt'
user_complete_file = 'csvs/User.csv'
naive_pickle = 'pickles/naive.pickle'
b_users_pickle = 'pickles/b_users.pickle'
final_rating_pickle = 'pickles/rating_matrix_final.pickle'


def parseAttributes(x):
    '''
        mapper for business attributes str->list of integers
    '''
    b_id = x[0]
    attr = map(int, x[1].split())
    return (b_id, attr)

def randomSelect(l):
    '''
        Selects the class with max probability
        ties are broken randomly
    '''
    m = max(l)
    c = l.count(m)
    if c == 1:
        return l.index(m) + 1
    r = random.randint(1,c)
    cnt = 0
    for j in range(5):
        if l[j] == m:
            cnt+=1
        if cnt == r:
            return j+1
    return l.index(m)+1

def calculateNB(x):
    '''
        Calculate the rating for user u : x[0], and business b: x[1]
        using the naive bayes model: User Preference 
        returns list of probabilities for all ratings 1-5
    '''
    u = x[0]
    b = x[1]
    probabilities = [0] * 5
    if priors_dict.has_key(u):
        rating_probabilities = priors_dict[u]
    else:
        rating_probabilities = [0.2] * 5
    for r in range(1, 6, 1):
        if attributes_probabilitiy_dict.has_key(u+"~"+str(r)):
            attributes_probability = attributes_probabilitiy_dict[u+"~"+str(r)]
        else:
            attributes_probability = [0.5] * num_of_attributes
        attributes = business_attr_dict[b][1]
        prob = math.log(rating_probabilities[r-1])
        i=0
        for attr in attributes:
            prob = prob + attr*math.log(attributes_probability[i]) + (1 - attr) * math.log(1 - attributes_probability[i])
            i+=1
        probabilities[r-1] = prob
    return probabilities

def getRating(user, business_id):
    '''
        returns rating so far computer for user-business pair
    '''
    bindex = business_attr_dict[business_id][0]
    uindex = user_avg_rating[user][0]
    rating = 1.0
    if rating_train_dict.has_key(user):
        rating = rating_train_dict[user]
    else:
        key = user+"_"+business_id
        if naive_matrix[bindex][uindex] <= 0.0:
            probabilities = calculateNB((user, business_id))
            rating = randomSelect(probabilities)
            naive_matrix[bindex, uindex] = rating
        else:
            rating = naive_matrix[bindex, uindex]
    return rating

def generateBusinessUserRatings():
    '''
        Initialize the rating matrix with original values if known
        else with the naive bayes estimation
    '''
    business_list = business_attr_dict.keys()
    users_list = user_businesses.keys()
    total = len(business_list)* len (user_avg_rating.keys())
    rating_matrix = np.zeros(( len(business_list),len(user_avg_rating) ))
    cnt = 0
    for business in business_list:
        bindex = business_attr_dict[business][0]
        for user in users_list:
            uindex = user_avg_rating[user][0]
            cnt+=1
            sys.stdout.write('\rProgress: %0.3f '%(float(cnt)*100/total)+"%")
            sys.stdout.flush()
            if rating_matrix[bindex, uindex] <= 0.0:
                rating_matrix[ bindex, uindex ] = getRating(user, business)
            if user not in graph_dict:
                continue
            friends = graph_dict[user]
            for friend in friends:
                findex = user_avg_rating[friend][0]
                if rating_matrix[bindex,findex] <= 0.0:
                    rating_matrix[bindex, findex] = getRating(friend, business)
        #b_users[business] = u_ratings
    f = open("pickles/naive.pickle", "w")
    pickle.dump(naive_matrix, f)
    #f.close()
    f = open("pickles/b_users.pickle", "w")
    pickle.dump(rating_matrix, f)
    f.close()
    return rating_matrix

#generateBusinessUserRatings()

def getDistantFriendsRating(user, friends, business_id):
    '''
        Perform the distant friend inference for one user
    '''
    rating = 0.0
    total_prob = 0.0
    flag = False
    bindex = business_attr_dict[business_id][0]
    uindex = user_avg_rating[user][0]
    for r in range(1, 6, 1):
        product = 1.0 * r
        for friend in friends:
            findex = user_avg_rating[friend][0]
            if rating_train_dict.has_key(friend+"_"+business_id):
                if user+"_"+friend in correlation_dict and rating_matrix[bindex,findex]>0:
                    flag = True
                    rvi =  rating_matrix[bindex,findex]
                    k_rvi = correlation_dict[user+"_"+friend][r - rvi] 
                    length = correlation_dict[user+"_"+friend]['l'] 
                    product *= (1.0 * k_rvi) / length
        rating += product
        total_prob += product/r
    if flag:
        if total_prob == 0.0:
            rating_matrix[bindex,uindex] = getRating(user, business_id)
        else:
            rating_matrix[bindex,uindex] = rating/total_prob
    else:
        rating_matrix[bindex,uindex] = getRating(user, business_id)

def generateFinalRatings():
    '''
        Perform distant friend inference
        Driver function
    '''
    M = 10
    total = M * len(business_attr_dict) * len (user_avg_rating)*1.0
    cnt = 0
    users = user_avg_rating.keys()
    for business_id in business_attr_dict.keys():
        #users = user_avg_rating.keys()
        for m in range(1, M+1, 1):
            random.shuffle(users)
            for user in users:
                cnt+=1
                sys.stdout.write("\r Progress:  Iteration: %d Total: %0.3f"%(m,cnt/total*100.0)+"%")
                sys.stdout.flush()
                if not rating_train_dict.has_key(user+"_"+business_id):
                    if user not in graph_dict:
                        continue
                    friends = graph_dict[user]
                    getDistantFriendsRating(user, friends, business_id)
    f = open(final_rating_pickle,'w')
    pickle.dump(rating_matrix,f)
    f.close()
#genarateFinalRatings()

def evaluateResults():
    '''
        Calculate rating classification results
    '''
    correct = 0
    error = 0.0
    count = 0 
    for user in user_businesses.keys():
        uindex = user_avg_rating[user][0]
        for business in user_businesses[user]:
            bindex = business_attr_dict[business][0]
            count += 1
            key = user + "_" + business
            actual = rating_dict[key]
            predicted = round(rating_matrix[bindex,uindex])
            error += abs(int(actual) - int(predicted))
            if(int(actual) == int(predicted)):
                correct += 1
    return error*1.0/count, correct*1.0/count

def getRecommendationList(user):
    '''
        Generate recommendation for user whose predicted ratings > average rating for the user
    '''
    uindex = user_avg_rating[user][0]
    ratings = sorted([(b,rating_matrix[business_attr_dict[b][0] , uindex]) for b in business_attr_dict.keys() if user+"_"+b not in rating_train_dict],key = lambda t : -t[1])
    avg_rating = user_avg_rating[user][1]
    recommendations_list = [x[0] for x in ratings if x[1] >= avg_rating]
    return recommendations_list

def evaluateScore(user, recommendations_list):
    '''
        Compute the score as the ratio of the common businesses in the test data and recommendations to the total businesses in test data
    '''
    business_list = user_businesses[user]
    if len(business_list) == 0:
        return 0.0
    common_recommendations = set(recommendations_list).intersection(set(business_list))
    score = float(len(common_recommendations)) / len(business_list)
    return score

def evaluateRecommendations():
    '''
        Driver program for evaluating recommendations
    '''
    score = 0.0
    cnt = 0
    for user in user_businesses.keys():
        score += evaluateScore(user, getRecommendationList(user))
        cnt += 1
    return score / cnt

############## Driver Code ###########################

'''
    Create a temporary table for Ratings. 
    TABLE: Ratings:
    Columns: 
        review_id: # unique identifier for reviews
        user_id: ID of the user
        business_id: ID of the business
        stars: Rating in stars
'''

ratings  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(ratings_file)
ratings.registerTempTable("Ratings")

'''
    Create a temporary table for Businesses
    TABLE: Business
    Columns:
        business_id: Unique business ID
        attributes: Attributes for businesses space separated binary integer (0/1)
        encodes features like categories, parking, waiter service, good for kids, ambience, good for lunc,breakfast etc....    
'''

business  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_file)
business.registerTempTable("Business")


'''
    Create a temporary table for Friend relationships user<->friend in form of list of edges
    TABLE: USER_FRIENDS
    Columns:
        user: USER ID
        friend: FRIEND USER ID
'''

user_friends = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_friends_file)
user_friends.registerTempTable("User_Friends")

'''
    Create a temporary file for User
    TABLE: USER_DATA
    Columns:
        user: USER ID
        average_stars: average rating of the user
'''


user_complete = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_complete_file)
user_complete.registerTempTable("User_Data")

# Collect the actual ratings for the users in User data
user_rating_list = sqlContext.sql("SELECT user_id, business_id, stars FROM Ratings ORDER BY user_id")
user_rating_actual = user_rating_list.map(lambda p: p[2]).collect()

'''
    user_avg_rating => dictionary
    key = user_id
    value = tuple (numerical_index, average_stars )

    numerical_index is used to map a user_id into a number 
    This number is used to index into numpy matrices later
'''
users_complete_data = sqlContext.sql("SELECT user_id, average_stars FROM User_Data")
users_avg_rating_rdd = users_complete_data.map(lambda x: (x.user_id, x.average_stars))
user_avg_rating = {}

cnt = 0
for value in users_avg_rating_rdd.collect():
    user_avg_rating[value[0]] = ( cnt ,float(value[1]))
    cnt += 1

'''
    businesses_attr_dict => dictionary
    key = business_id
    value = ( numeric_index, list of binarized attribute values)

    numerical_index is used to map a business_id into a number 
    This number is used to index into numpy matrices later
    
'''

businesses = sqlContext.sql("SELECT * FROM Business")

business_attr = businesses.map(parseAttributes)

business_attr_dict = {}
cnt = 0
for value in business_attr.collect():
    business_attr_dict[value[0]] = (cnt,value[1])
    cnt+=1

num_of_attributes = len(value[1])


#Load training information
'''
    Prior probabilites that the user will rate a business
    priors_dict => dictionary
    key = user_id
    value = list of 5 probabilities [Pr(r=1), Pr(r=2), Pr(r=3), Pr(r=4), Pr(r=5)] 

'''

f = open(priors_pickle)
priors_dict = pickle.load(f)
f.close()

'''
    Conditional probabilites for attributes that the user will rate a business
    priors_dict => dictionary
    key = user_id+'~'+rating
    value = list of probabilities of all attributes value=1 [Pr(Ai = 1) for all in range(num_of_attributes)]
'''

f = open(attributes_prob_pickle)
attributes_probabilitiy_dict = pickle.load(f)
f.close()

'''
    Load Rating Correlations between friends
    correlation_dict : 
    key = user_id+'_'+friend_user_id
    value = dictionary of probability of rating difference = i for i in (-4,4)
'''

f = open(correlation_pickle)
correlation_dict = pickle.load(f)
f.close()

'''
    Load adjacency list of friends into graph_dict
'''

f = open(graphs_pickle)
graph_dict = pickle.load(f)
f.close()

'''
    Load rating dictionary for test data
    key = user_id+"_"+business_id
    value = rating
'''

f = open(rating_dict_file)
rating_dict = pickle.load(f)
f.close()

'''
    Load rating dictionary for train data
    key = user_id+"_"+business_id
    value = rating
'''

f = open(ratings_train_dict_file)
rating_train_dict = pickle.load(f)
f.close()

'''
    user -> rated businesses mapping 
    key = user_id
    value = list of businesses user has rated 
'''

f = open(user_businesses_file)
user_businesses = pickle.load(f)
f.close()

'''
    Load matrices calculated for faster computation
    Generation of these matrices are computation intensive
    
    Rows =>     Business 
    Columns =>  Users  

    naive_matrix : Calculates Naive Bayes Probability for user business pairs
    rating_matrix : Initial rating matrix before Distant Friend Inference is applied

'''

try:
    f = open(naive_pickle)
    naive_matrix = pickle.load(f)
    f.close()
    f = open(b_users_pickle)
    rating_matrix = pickle.load(f)
    f.close()
except:
    naive_matrix = np.zeros( (len(business_attr_dict),len(user_avg_rating)) )        
    rating_matrix = generateBusinessUserRatings()    
'''
    Load the rating matrix after distant friend inference
    rating_matrix : final rating matrix
'''

try:
    f = open(final_rating_pickle)
    rating_matrix = pickle.load(f)
    f.close()
except:
    generateFinalRatings()

print 

err,acc = evaluateResults()
print "accuracy for classification", acc
print "mean absolute error", err

accuracy = evaluateRecommendations()
print "accuracy : ", accuracy

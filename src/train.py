from pyspark.sql import SQLContext
import scipy
import numpy as np
from scipy.stats import pearsonr
import pickle

#-------------------------------------------------------------------------------------------------------------------------

"""
	Context for Spark SQL
"""
sqlContext = SQLContext(sc)

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Input files for loading the data 
"""
users_file = 'csvs/User.csv'
ratings_file = 'csvs/Ratings_train.csv'
business_file = 'csvs/Business.csv'
user_friends_file = 'csvs/edges_train.txt'
rating_dict_file = 'pickles/rating_train_dict.pickle'
user_businesses_file = 'pickles/user_businesses_train.pickle'

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Creating schema in Spark SQL and loading the data 
"""
user = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(users_file)
user.registerTempTable("Users")

ratings  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(ratings_file)
ratings.registerTempTable("Ratings")

business  = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(business_file)
business.registerTempTable("Business")

user_friends = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(user_friends_file)
user_friends.registerTempTable("User_Friends")

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Loading the data from serialized pickle object 
"""
f = open(rating_dict_file)
rating_dict = pickle.load(f)
f.close()

f = open(user_businesses_file)
user_businesses = pickle.load(f)
f.close()

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Fetching user and his friend information from SQL table 
"""
user_friends_list = sqlContext.sql("SELECT * FROM User_Friends")

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Converting user_friends SQl DataFrame RDD to Spark RDD 
"""
user_friends_rdd = user_friends_list.map(lambda p: (p.user, p.friend))

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Fetching user and his cumulative count for each rating from SQL table 
"""
user_rating_list = sqlContext.sql("SELECT user_id, stars, count(*) cnt FROM Ratings GROUP BY user_id, stars ORDER BY user_id")

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Converting user_ratings SQl DataFrame RDD to Spark RDD 
"""
user_rating_dict = user_rating_list.map(lambda x: (x.user_id,(x.stars,x.cnt))).groupByKey()

#-------------------------------------------------------------------------------------------------------------------------

"""
	It calculates the total sum for all the ratings for the user
	e.g. (used_id, 10, [(1, 2), (2, 3), (3, 1), (4, 1), (5, 3)])
"""
def sumCalculator(x):
	sum = 0
	for val in x[1]:
		sum = sum + val[1]
	return (x[0], sum, x[1])

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates a RDD by mapping user to total sum of ratings and rating list of each rating
	e.g. (used_id, 10, [(1, 2), (2, 3), (3, 1), (4, 1), (5, 3)])
"""
user_rating_update = user_rating_dict.map(sumCalculator)

#-------------------------------------------------------------------------------------------------------------------------

"""
	It calculates the probability of each rating for the user
	e.g. (used_id, [0.2, 0.2, 0.3, 0.1, 0.2])
"""
def getProbability(x):
	list_probability = [(1.0 / (x[1] + 5))] * 5
	for val in x[2]:
		list_probability[(val[0] - 1)] = ((1.0 + val[1])/ (x[1] + 5))
	return (x[0], tuple(list_probability))

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates a RDD by mapping user to probability of each rating
	e.g. (used_id, [0.2, 0.2, 0.3, 0.1, 0.2])
"""
user_rating_priors = user_rating_update.map(getProbability)

#-------------------------------------------------------------------------------------------------------------------------

""" 
	Fetching list of businesses attributes for a particular user and rating combination 
	e.g. (user_id~rating1, "1 0 0 1")
	e.g. (user_id~rating2, "0 1 0 1")
"""
user_rating_businesses = sqlContext.sql("SELECT concat(r.user_id, '~', r.stars) as user_stars, b.attributes FROM Ratings r, Business b WHERE r.business_id = b.business_id ORDER BY user_id")

#-------------------------------------------------------------------------------------------------------------------------

"""
	It parses the attributes of businesses from string to list
	e.g. Input : (user_id~rating, "1 0 0 1")
		 Output : (user_id~rating, [1, 0, 0, 1])
"""
def parseAttributes(x):
	user_star = x[0]
	attr = map(int, x[1].split())
	return (user_star, attr)

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates a RDD by mapping user and ratings to business atrributes
	e.g. (user_id~rating, [[1, 0, 0, 1], [1, 1, 0, 0]])
"""
user_rating_bussiness_attr = user_rating_businesses.map(parseAttributes).groupByKey()

#-------------------------------------------------------------------------------------------------------------------------

"""
	It calculates the sum of all the attributes
	e.g. (user_id~rating, [2, 1, 0, 1], [[1, 0, 0, 1], [1, 1, 0, 0]])
"""
def getSumOfAttributes(x):
	return (x[0], map(sum, zip(*x[1])), x[1])

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates a RDD by mapping user to business atrributes and their respective total
	e.g. (user_id~rating, [2, 1, 0, 1], [[1, 0, 0, 1], [1, 1, 0, 0]])
"""
user_rating_bussiness_attr_sum = user_rating_bussiness_attr.map(getSumOfAttributes)

#-------------------------------------------------------------------------------------------------------------------------

"""
	It calculates probabilties of the business attributes for user and ratings
	e.g. (user_id~rating, [0.5, 0.3, 0.4, 0.2])
"""
def getAttributesProbability(x):
	list_probability = []
	for index in range(0, len(x[1]), 1):
		probability = (x[1][index] + 1.0) / (len(x[2]) + 2)
		list_probability.append(probability)
	return (x[0], list_probability)

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates a RDD by mapping user and ratings to probabilties of the business attributes
	e.g. (user_id~rating, [0.5, 0.3, 0.4, 0.2])
"""
user_rating_bussiness_attr_prob = user_rating_bussiness_attr_sum.map(getAttributesProbability)

#-------------------------------------------------------------------------------------------------------------------------

"""
	It calculates differences between the rating given by user and his friend to same business
	e.g. (user_id, friend_id, total_common_businesses, {-4: 3, ...., 4: 5})
	difference can vary from -4 to 4
"""
def differenceCalculator(u_v_pair):
	u = u_v_pair[0]
	v = u_v_pair[1]
	u_businesses = user_businesses[u]
	v_businesses = user_businesses[v]
	final_businesses = u_businesses.intersection(v_businesses) 
	if len(final_businesses) == 0:
		return (u,v,0,{})
	rating_u = [float(rating_dict[u+"_"+k]) for k in final_businesses ]
	rating_v = [float(rating_dict[v+"_"+k]) for k in final_businesses ]
	x = np.array(rating_u)
	y = np.array(rating_v)
	diff = x-y
	dictCorr = { i : 0 for i in range(-4,5)}
	for item in diff:
		dictCorr[int(item)] += 1 
	return (u, v, len(final_businesses), dictCorr)

#-------------------------------------------------------------------------------------------------------------------------

"""
	Creates RDD for mapping differences between the rating given by user and his friend to same business
	e.g. (user_id, friend_id, total_common_businesses, {-4: 3, ...., 4: 5})
	difference can vary from -4 to 4 and filtering those friends that have less than 3 in common.
"""
user_friends_correlation = user_friends_list.map(differenceCalculator).filter(lambda p: p[2]>=3)

#-------------------------------------------------------------------------------------------------------------------------

"""
	Calculates the correlation between the rating given by user and his friend to same business
	e.g. (user_id, friend_id, {-4: 3, ...., 4: 5})
"""
dictCorr = {}
for item in user_friends_correlation.collect():
	dictCorr[item[0]+"_"+item[1]] = item[3]
	dictCorr[item[1]+"_"+item[0]] = {-k:v for k,v in item[3].items()}
	dictCorr[item[0]+"_"+item[1]]["l"] = item[2]
	dictCorr[item[1]+"_"+item[0]]["l"] = item[2]

#-------------------------------------------------------------------------------------------------------------------------

"""
	Saves the results in the serialized pickle so that it can be used in testing
"""
priors_pickle = open("pickles/priors.pickle", "w")
priors_dict = {}
for value in user_rating_priors.collect():
	priors_dict[value[0]] = value[1]
pickle.dump(priors_dict, priors_pickle)

attributes_prob_pickle = open("pickles/attributes_probability.pickle", "w")
attributes_prob_dict = {}
for value in user_rating_bussiness_attr_prob.collect():
	attributes_prob_dict[value[0]] = value[1]
pickle.dump(attributes_prob_dict, attributes_prob_pickle)

correlation_pickle = open("pickles/correlation.pickle", "w")
pickle.dump(dictCorr, correlation_pickle)

#-------------------------------------------------------------------------------------------------------------------------
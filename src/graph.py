import csv

relevantUsers = {}
with open('User.csv', 'rb') as csvfile:
	lines = csv.reader(csvfile, delimiter=',')
	for item in lines:
	    relevantUsers[item[0]] = True

alreadyAdded = {}	

with open('Friend.csv', 'rb') as csvfile:
	f = open("Edges.txt", 'w')
	for lines in csvfile:
		lis = lines.split(",",1)
		if lis[0] in relevantUsers:
			friends =  lis[1][1:-2].split(",")
			friendList = [x.strip()[2:-1] for x in friends if relevantUsers.has_key(x.strip()[2:-1]) and x.strip()[2:-1] is not '']
			for friend in friendList:
				if alreadyAdded.has_key(friend+"_"+lis[0].strip()):
					continue
				else:					
					alreadyAdded[lis[0].strip()+"_"+friend] = True
					f.write(lis[0].strip()+","+friend+ '\n')

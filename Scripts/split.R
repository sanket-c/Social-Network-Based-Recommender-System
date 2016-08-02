library(caTools)

Ratings = read.csv('../csvs/Ratings.csv',header=T)
#print(Ratings)
split = sample.split(Ratings$stars, SplitRatio = 0.75)
train = subset(Ratings, split == TRUE)
test = subset(Ratings,split == FALSE)
write.csv(train, '../csvs/Ratings_train.csv',row.names = FALSE,quote=F)
write.csv(test, '../csvs/Ratings_test.csv',row.names = FALSE,quote=F)



# Social-Network-Based-Recommender-System
A Recommender System that makes use of friendship relations in a social network graph to improve recommendation accuracy.

----------------------------------------------------------------
Tools/Libraries:
----------------------------------------------------------------
Apache SPARK

SparkSQL

R library caTools

scipy

numpy

----------------------------------------------------------------
Important Links:
----------------------------------------------------------------
Paper: http://www.cobase.cs.ucla.edu/tech-docs/jmhek/snrs.pdf

Dataset: https://www.yelp.com/dataset_challenge

Filtered Dataset: https://drive.google.com/open?id=0B88KCEO9WlRUVmtqMGpZZnRVaW8 

-----------------------------------------------------------------
Setup:
-----------------------------------------------------------------
1) Clone the repository specified in the Github link

2) Download and unzip Filtered Dataset. Copy the content of the csvs folder in the zip into the csvs folder into the repository.

3) Copy the content of the pickles folder in the zip to the pickles folder in the repository.

The folder structure should look like this:--

Folder structure:

SNRS

-- src

---- train.py

---- test.py 

-- csvs

---- (csv files....)

-- Scripts

---- pre_process.py

---- split.R

-- pickles

---- (pickle files....)

-- Dataset

---- user.json

---- business.json

---- review.json


-----------------------------------------------------------------
Preprocessing instructions:
-----------------------------------------------------------------
1) cd to SNRS/scripts

2) Run "python pre_process.py <city-name>"

3) A category list is generated in the middle of pre-processing. The script will pause allowing the user to change the categories that need to be considered as features.Press enter once the category list is finalised.

4) All necessary files for training will be created.

-----------------------------------------------------------------
Execution Instructions:
-----------------------------------------------------------------
1) cd to the directory SNRS/

2) Run pyspark: 'IPYTHON=1 /home/sanket/apache-spark/spark-1.5.2-bin-hadoop2.6/bin/pyspark --packages com.databricks:spark-csv_2.10:1.3.0'

3) Run the training: execfile('src/train.py')

4) Run the test: execfile('src/test.py')  

-----------------------------------------------------------------

NOTES:
1) Execution takes a lot of time. Intermediate calculates are stored in python pickle formats

2) The filtered dataset and created pickles for 'Tempe' can be found here: 

a) pickles/naive.pickle 
b) pickles/b_users.pickle
c) pickles/rating_matrix_final.pickle

3) If the algorithm should be run for a different city, run pre_process.py with the city name and delete the following files:

a) pickles/naive.pickle 
b) pickles/b_users.pickle
c) pickles/rating_matrix_final.pickle

Follow the execution instructions.

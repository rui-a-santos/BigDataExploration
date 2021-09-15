from pyspark import SparkContext
from operator import add

sc=SparkContext('local', 'pyspark')

def age_group(age):
	if age < 10:
		return '0-10'
	elif age < 20:
		return '10-20'
	elif age < 30:
		return '20-30'
	elif age < 40:
		return '30-40'
	elif age < 50:
		return '40-50'
	elif age < 60:
		return '50-60'
	elif age < 70:
		return '60-70'
	elif age < 80:
		return '70-80'
	else:
		return '80+'

def parse_with_age_group(data):
	userid,age,gender,occupation,zip = data.split("|")
	return userid,age_group(int(age)),gender,occupation,zip,int(age)

#Imports File
fs=sc.textFile("file:///home/cloudera/Desktop/u.user")

#Maps data from file
data_with_age_group=fs.map(parse_with_age_group)

#Gets data for specific age groups
data_with_age_40_50 = data_with_age_group.filter(lambda x: '40-50' in x)
data_with_age_50_60 = data_with_age_group.filter(lambda x: '50-60' in x) 

#Reduces all the specified data by key and sorts in descending
#Code inspired from methods in the book: Big Data Science & Analytics - A hands on Approach by Arshdeep Bahga and Vijay Madisetti -> Section 7.6. Page 252-256
data_with_age_40_50_reduced = data_with_age_40_50.map(lambda x: (x[3],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: -a[1])
data_with_age_50_60_reduced = data_with_age_50_60.map(lambda x: (x[3],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: -a[1])

#Creates a map of only the first column (Eliminating the count value)
data_with_age_40_50_remove_count = data_with_age_40_50_reduced.map(lambda x: x[0])
data_with_age_50_60_remove_count = data_with_age_50_60_reduced.map(lambda x: x[0])

#Getting the top 10 per occupation (They are already sorted from a previous step)
data_with_age_40_50_top_10 = sc.parallelize(data_with_age_40_50_remove_count.take(10))
data_with_age_50_60_top_10 = sc.parallelize(data_with_age_50_60_remove_count.take(10))

#Prints the intersection of the jobs of both age groups
print(data_with_age_40_50_top_10.intersection(data_with_age_50_60_top_10).collect())


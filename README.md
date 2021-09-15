# BigDataExploration
This project implements advanced queries to fulfil complex real world stakeholder questions.

- Task1 folder contains MongoDB queries which are stored in text files. The querries are aplied to the championsleague.json file.
  - Query1 returns the name, number of followers, and number of friends, of each user with fewer than 25 friends, whose name starts with "A" (case insensitive) and ends with "es" (case sensitive)
  - Query2 returns the average ratio between numbers of followers and number of friends, over all documents
  - Query3 returns the number of users who have at least 1000 friends, posted a tweet and whose tweet contains the string “Madrid”.

- Task2 folder contains Apache Spark code. The task2.py file gets all occupations that are performed by users in the age group [40,50) and by users in the age group [50,60) and are among the 10 most frequent occupations for the users in each age group. The output of the code is seen in task2.out.

- Task3 folder contains HiveQL queries that are applied to the query_logs.txt file. 
  - Query1 returns the maximum number of visits of all users. 
  - Query2 returns all attributes of each user who issues a query that contains the string "job". 
  - Query3 returns the number of distinct users who issue a (non-empty) query between 21:00:00 (inclusive) and 22:59:59 (inclusive).

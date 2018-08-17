## Description

The application collects tweets from feed of major news agencies and identifies best tweets which can then be notified to the users.

## Methodology

The script will execute every 10 mins, collects and save 100 most recent tweets from every twitter handle provided in the config file. After preprocessing and cleaning of data, feature vectors are extracted using pre-trained word2vec having 100 dimensions. After that tweets are clustered into events. The clusters having more data points are considered hot and then best tweets from two such clusters are identified and saved using a scoring parameter which is a comination of number of retweets and favorites.


## Technology Stack
	
	1. Python - v2.7+
	2. Apache Spark - 2.1.0
	3. Linux

## Pre-requisites

	1. Download the latest standalone spark binary and set appropriate environment variables. Check [here](https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm) for details
	2. Install python v2.7+ and modules tweepy and gensim using pip


## Running Instructions

	Clone the repository in your local machine and run either: 

		Manually:

		spark-sbmit <params> main.py.

		e.g: spark-submit --master=yarn-cluster main.py or spark-submit --master=local[*] main.py

		Using cron job:
		
		1. Replace current configuration parameters with your values in config.ini file
		2. Create a new cron tab
			crontab -e 
		3. Add following line to create a cron job that runs every 10 mins 
			*/10 * * * * spark-submit <params> main.py

## Notes

	1. For word2vec Google's pre-trained model is better and should be used for production systems, however its memory heavy for local execution
	2. Here number of clusters are kept to 10, however there are various techniques for identifying optimal cluster size 'elbow method' being one of them. Since those processes would have added latency therefore not applied here

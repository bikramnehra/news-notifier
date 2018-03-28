from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import udf, col, max
from pyspark.mllib.clustering import KMeans, KMeansModel
import gensim
import configparser

## Reading config parameters
config = configparser.ConfigParser()
config.read('config.ini')
w2v_model_path = config.get('paths','model')
results_path = config.get('paths','results')

# Setting w2v pre-trained model
model = gensim.models.KeyedVectors.load_word2vec_format(w2v_model_path, binary=True)

def transform_word(wordList):
    tempList = []
    for word in wordList:
	try:
            tempList.append(model[word])
        except Exception as e:
            print e
    if len(tempList) > 0:    
    	return sum(tempList)/len(tempList)

def analyze(file_path):
	#initilaizing spark and sql context
	conf = SparkConf().setAppName('analyzer')
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	# Reading the tweets data
	fields = [StructField("id", StringType(), True), \
          	  StructField("created_at", StringType(), True), \
                  StructField("text", StringType(), True), \
                  StructField("retweet_count", IntegerType(), True), \
                  StructField("favorite_count", IntegerType(), True)]

        schema = StructType(fields)
        tweetsDF = sqlContext.read.csv(file_path, header='true', schema=schema).cache()
	
	# Splitting tweets
	tweetsTextSplitted = tweetsDF.select("id","text") \
                             .where("text is not NULL").rdd \
                             .map(lambda (key,val): (key,val.split()))
    
   	# Removing urls from tweets
	tweetsTextCleaned = tweetsTextSplitted.map(lambda (key,val): (key, [x for x in val if not 'https://' in x]))
	
	# extracting feature vector
	tweetsTextFV = tweetsTextCleaned.map(lambda (key,val): (key,transform_word(val))) \
                                .filter(lambda (key,val): (val is not None)).cache()

	# Training K-means
	clusterModel = KMeans.train(tweetsTextFV.values(), 10)

	# Assigning tweets to clusters
	clusteredTweets = tweetsTextFV.map(lambda (key,val): (key, (clusterModel.predict(val))))


	fields = [StructField("id", StringType(), True), StructField("cluster", IntegerType(), True)]
	schema = StructType(fields)
	clusteredTweetsDF = clusteredTweets.toDF(schema=schema)
	joinedTweetsDF = tweetsDF.join(clusteredTweetsDF, "id").cache()


	# finding hot events by giving out clusters with maximum sizes
	topEventsDF = joinedTweetsDF.groupBy("cluster").count() \
                            .orderBy(col("count").desc()).limit(2)


	# Adding new column score by adding retweet_count & favorite_count
	scoreDF = joinedTweetsDF.join(topEventsDF, "cluster") \
                        .withColumn("score", col("retweet_count")+col("favorite_count"))


    #UDF for generating tweet URL
   	urlUDF = udf(lambda x: "https://twitter.com/statuses/"+x)

	# Finding tweets from clusters which has maximum score value
	resultsDF = scoreDF.groupBy("cluster").agg(max("score").alias("score")) \
        			   .join(scoreDF, "score") \
                       .select("id", "text", "created_at", "retweet_count", "favorite_count") \
                       .withColumn("tweet_URL", urlUDF(col("id")))

    # Saving results into a text file
	resultsDF.distinct().rdd.coalesce(1).saveAsTextFile(results_path+"-"+file_path.split("-")[1].split(".")[0])

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
import re
import csv, io

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

schema = StructType([
     StructField("review_id", StringType()),
     StructField("user_id", StringType()),
     StructField("business_id", StringType()),
     StructField("stars", IntegerType()),
     StructField("date", StringType()),
     StructField("text", StringType()),
     StructField("useful", IntegerType()),
     StructField("funny", IntegerType()),
     StructField("cool", IntegerType())
])

rdd = spark.read.json('review.json', schema).rdd
rdd.first()
rdd2 = rdd.map(lambda r: (r.business_id, r.review_id, r.text))
rdd2.take(100)

rdd_l =  sc.textFile('values.txt')
rdd_l = rdd_l.map(lambda x: (x.split(',')[0], x.split(',')[1]))
rdd_l.take(6000)

lexicon_dict = rdd_l.collectAsMap()
lexicon = sc.broadcast(lexicon_dict)         

def calculateSentiment(text, lexicon):
    sentiment = 2.5
    text = re.sub("[\"#$%^&*@\\-=:;?().,]", "", text)
    words = text.split(" ")
    for word in words:
        isExtreme= False
        if "!" in word:
            word = re.sub("!", "", word)
            isExtreme = True
        if word in lexicon:
            lexiconValue = float(lexicon[word])
            bonus = (0.5 + lexiconValue) if isExtreme else 0
            sentiment = sentiment + lexiconValue + bonus
    return sentiment
    
rdd3 = rdd2.map(lambda x: (x[0], calculateSentiment(x[2], lexicon.value)))
rdd3.take(100)


rdd4 = rdd3 \
    .mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: v[0]/v[1]) 
rdd4.take(100)

def to_csv(x):    
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip() # remove extra newline

# ... do stuff with your rdd ...
rdd5 = rdd4.map(to_csv)
rdd5.saveAsTextFile("sentiment.csv")
rdd5.take(10)
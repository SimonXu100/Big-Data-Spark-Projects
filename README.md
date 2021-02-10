# Big-Data-Spark-Projects

1 Spark: WordCount Spark application

Create EMR cluster configured for Spark

Go through three ways to calculate word count using spark.

Way 1 : SSH and run script
	scp –i <SECRET-KEY> wordcount.py user@<master end-point>
	ssh –i <SECRET-KEY> user@<master end-point>
	spark-submit wordcount.py | tee output.txt
	
Way 2 : Use Lambda to run Spark
	
Way 3 : Using Amazon EMR, go to notebook on the Amazon EMR page , create notebook and attach to the cluster you created.

2 Spark streaming demo:
kafka + Spark Streaming
![image](https://github.com/SimonXu100/Big-Data-Spark-Projects/blob/main/Spark%20Streaming%20demo/spark%20streaming%20pipeline.jpeg)

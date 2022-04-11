from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
    StructField("InvoiceNo",  StringType(),  True),
    StructField("StockCode", StringType(),  True),
    StructField("Description",  StringType(),   True),
    StructField("Quantity",  IntegerType(),   True),
    StructField("InvoiceDate",  StringType(), True),
    StructField("UnitPrice",   StringType(), True),
    StructField("CustomerID",  IntegerType(),  True),
	StructField("Country",  StringType(),  True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))

	df = df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'),',','.').cast('float'))
	#print(df.filter(F.col('StockCode').contains('gift_0001')).show())

	df.createOrReplaceTempView('df_online_retail')

def p1_OR ():
	print(spark.getOrCreate().sql(f"""
	SELECT  ROUND(SUM(Quantity*UnitPrice),2) as Valor_Gift_Card
	FROM df_online_retail
	WHERE StockCode like 'gift_0001_%'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	""").show())

p1_OR()



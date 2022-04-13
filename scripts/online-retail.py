import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
 
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

	
	#SUBSTITUIR "," POR "." E CONVERVETER VALOR PARA FLOAT
	df = df.withColumn('UnitPrice', F.regexp_replace(F.col('UnitPrice'),',','.').cast('float'))

	#FORMATAR DATA NO FORMATO d/M/yyyy H:m E CONVERTER PARA TimestampType
	df = (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m')))

	#CRIAR CAMPO SOLD (VALOR VENDIDO Quantity*UnitPrice)
	df = (df.withColumn('Sold', F.col("Quantity")*F.col("UnitPrice")))

	#print(df.show())

	df.createOrReplaceTempView('df_online_retail')

def p1_OR ():
	print(spark.getOrCreate().sql(f"""
	SELECT  ROUND(SUM(Quantity*UnitPrice),2) as Valor_Gift_Card
	FROM df_online_retail
	WHERE StockCode like 'gift_0001_%'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	""").show())

#p1_OR()

def p2_OR ():
	print(spark.getOrCreate().sql(f"""
	SELECT  Year(InvoiceDate) as Year,
			Month(InvoiceDate) as Month,
			ROUND(SUM(Quantity*UnitPrice),2) as Valor_Gift_Card
	FROM df_online_retail
	WHERE StockCode like 'gift_0001_%'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	GROUP BY Year(InvoiceDate),
			 Month(InvoiceDate)
	ORDER BY 1, 2
	""").show())

#p2_OR()

def p3_OR ():
	print(spark.getOrCreate().sql(f"""
	SELECT  ROUND(SUM(Quantity*UnitPrice),2) as Valor_Sample
	FROM df_online_retail
	WHERE StockCode = 'S'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	""").show())

#p3_OR()

def p4_OR ():
	print((df.filter(~(F.col('InvoiceNo').startswith('C')) & ~(F.col('InvoiceNo').startswith('c')))
			 .groupBy('Description')
		     .agg(F.sum('Sold').alias('Max_Valor'))
		     .agg(F.max(F.struct(F.col('Max_Valor'),
							    F.col('Description').alias("Produto"))).alias('Max_Product'))
		     .select("Max_Product.Produto", F.round("Max_Product.Max_Valor",2).alias('Valor_Produto'))).show())

p4_OR()
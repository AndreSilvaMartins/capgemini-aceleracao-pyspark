from ast import Return
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

	#print(df.show())
	
#SUBSTITUIR "," POR "." E CONVERVETER VALOR PARA FLOAT
def Transformar_UnitPrice_Float(df):
	return df.withColumn('UnitPrice', F.round(F.regexp_replace(F.col('UnitPrice'),',','.').cast('float'),2))

#CRIAR CAMPO SOLD (VALOR VENDIDO Quantity*UnitPrice)
def Adicionar_Variavel_Sold(df):
	return (df.withColumn('Sold', F.round(F.col("Quantity")*F.col("UnitPrice"),2)))

#FORMATAR DATA NO FORMATO d/M/yyyy H:m E CONVERTER PARA TimestampType
def Transformar_InvoiceDate_TimeStamp(df):
	return (df.withColumn('InvoiceDate', F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m')))
	
#CRIAR TEMPVIEW PARA USO DO SQL
def Criar_TempView(df):
	return df.createOrReplaceTempView('df_online_retail')
	
def p1_OR ():
	print("Pergunta 1")
	print(spark.getOrCreate().sql(f"""
	SELECT  ROUND(SUM(Sold),2) as Valor_Gift_Card
	FROM df_online_retail
	WHERE StockCode like 'gift_0001_%'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	""").show())

def p2_OR ():
	print("Pergunta 2")
	print(spark.getOrCreate().sql(f"""
	SELECT  Year(InvoiceDate) as Year,
			Month(InvoiceDate) as Month,
			ROUND(SUM(Sold),2) as Valor_Gift_Card
	FROM df_online_retail
	WHERE StockCode like 'gift_0001_%'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	GROUP BY Year(InvoiceDate),
			 Month(InvoiceDate)
	ORDER BY 1, 2
	""").show())

def p3_OR ():
	print("Pergunta 3")
	print(spark.getOrCreate().sql(f"""
	SELECT  SUM(Quantity) as Quantidade_Sample
	FROM df_online_retail
	WHERE StockCode = 'S'
	AND SUBSTRING(InvoiceNo,1,1) <> 'C'
	AND SUBSTRING(InvoiceNo,1,1) <> 'c'
	""").show())

def p4_OR ():
	print("Pergunta 4")
	print((df.filter(~(F.col('InvoiceNo').startswith('C')) & ~(F.col('InvoiceNo').startswith('c') & (F.col('StockCode') != 'PADS')))
			 .groupBy('Description')
		     .agg(F.sum('Quantity').alias('Max_Quantidade'))
		     .agg(F.max(F.struct(F.col('Max_Quantidade'),
							    F.col('Description').alias("Produto"))).alias('Max_Product'))
		     .select("Max_Product.Produto", F.round("Max_Product.Max_Quantidade",2).alias('Quantidade_Produto'))).show(truncate=False))

def p5_OR ():
	print("Pergunta 5")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_QUANTIDADE
									AS
									(SELECT YEAR(InvoiceDate) AS Ano,
											MONTH(InvoiceDate) AS Mes,
											Description AS Produto,
											ROUND(SUM(Quantity),2) AS Quantidade_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									GROUP BY YEAR(InvoiceDate),
											 MONTH(InvoiceDate), 
											 Description)

									SELECT  Ano,
											Mes,
											Produto,
											Quantidade_Vendas as Quantidade_Vendido
									FROM MAX_QUANTIDADE a
									WHERE Quantidade_Vendas = (SELECT MAX(b.Quantidade_Vendas) FROM MAX_QUANTIDADE b WHERE a.Ano = b.Ano and a.Mes = b.Mes)
									ORDER BY 1, 2
									""").show())

def p6_OR ():
	print("Pergunta 6")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_VALOR
									AS
									(SELECT YEAR(InvoiceDate) AS Ano,
											MONTH(InvoiceDate) AS Mes,
											DAY(InvoiceDate) AS Dia,
											HOUR(InvoiceDate) AS Hora,
											ROUND(SUM(Sold),2) AS Valor_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									GROUP BY YEAR(InvoiceDate),
											 MONTH(InvoiceDate), 
											 DAY(InvoiceDate),
											 HOUR(InvoiceDate))

									SELECT  Hora,
											Valor_Vendas as Valor_Vendido
									FROM MAX_VALOR a
									WHERE Valor_Vendas = (SELECT MAX(Valor_Vendas) FROM MAX_VALOR)
									""").show())

def p7_OR ():
	print("Pergunta 7")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_VALOR
									AS
									(SELECT YEAR(InvoiceDate) AS Ano,
											MONTH(InvoiceDate) AS Mes,
											ROUND(SUM(Sold),2) AS Valor_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									GROUP BY YEAR(InvoiceDate),
											 MONTH(InvoiceDate))

									SELECT  Mes,
											Ano,
											Valor_Vendas as Valor_Vendido
									FROM MAX_VALOR a
									WHERE Valor_Vendas = (SELECT MAX(Valor_Vendas) FROM MAX_VALOR)
									""").show())

def p8_OR ():
	print("Pergunta 8")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_QUANTIDADE
									AS
									(SELECT YEAR(InvoiceDate) AS Ano,
											MONTH(InvoiceDate) AS Mes,
											Description AS Produto,
											ROUND(SUM(Quantity),2) AS Quantidade_Vendas,
											ROUND(SUM(Sold),2) AS Valor_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									GROUP BY YEAR(InvoiceDate),
											 MONTH(InvoiceDate), 
											 Description)
									
									SELECT Ano,
											Mes,
											Produto,
											Valor_Vendas as Valor_Vendido
									FROM MAX_QUANTIDADE a
									WHERE Quantidade_Vendas = (SELECT MAX(b.Quantidade_Vendas) FROM MAX_QUANTIDADE b WHERE a.Ano = b.Ano and a.Mes = b.Mes)
									AND Valor_Vendas = (SELECT MAX(Valor_Vendas) FROM MAX_QUANTIDADE)
									ORDER BY 1, 2
									""").show())

def p9_OR ():
	print("Pergunta 9")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_VALOR
									AS
									(SELECT Country AS Pais,
											ROUND(SUM(Sold),2) AS Valor_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									GROUP BY Country)
									
									SELECT  Pais,
											Valor_Vendas as Valor_Vendido
									FROM MAX_VALOR a
									WHERE Valor_Vendas = (SELECT MAX(Valor_Vendas) FROM MAX_VALOR)
									ORDER BY 1, 2
									""").show())

def p10_OR ():
	print("Pergunta 10")
	print(spark.getOrCreate().sql(f"""
									WITH MAX_VALOR
									AS
									(SELECT Country AS Pais,
											ROUND(SUM(Sold),2) AS Valor_Vendas
									FROM df_online_retail
									WHERE SUBSTRING(InvoiceNo,1,1) <> 'C'
									AND SUBSTRING(InvoiceNo,1,1) <> 'c'
									AND StockCode <> 'PADS'
									AND (StockCode = 'm'
									OR StockCode = 'M'
									OR RIGHT(StockCode,1) = 'm'
									OR RIGHT(StockCode,1) = 'M')
									GROUP BY Country)
									
									SELECT  Pais,
											Valor_Vendas as Valor_Vendido
									FROM MAX_VALOR a
									WHERE Valor_Vendas = (SELECT MAX(Valor_Vendas) FROM MAX_VALOR)
									ORDER BY 1, 2
									""").show())

df = Transformar_UnitPrice_Float(df)
df = Adicionar_Variavel_Sold(df)
df = Transformar_InvoiceDate_TimeStamp(df)
Criar_TempView(df)
#p1_OR()
#p2_OR()
#p3_OR()
#p4_OR()
#p5_OR()
#p6_OR()
#p7_OR()
#p8_OR()
#p9_OR()
p10_OR()


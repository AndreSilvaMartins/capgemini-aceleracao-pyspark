from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 
if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

    schema_census_income = StructType([
                                            StructField("age", IntegerType(), True),
                                            StructField("workclass", StringType(), True),
                                            StructField("fnlwgt", IntegerType(), True),
                                            StructField("education", StringType(), True),
                                            StructField("education-num", IntegerType(), True),
                                            StructField("marital-status", StringType(), True),
                                            StructField("occupation", StringType(), True),
                                            StructField("relashionship", StringType(), True),
                                            StructField("race", StringType(), True),
                                            StructField("sex", StringType(), True),
                                            StructField("capital-gain", IntegerType(), True),
                                            StructField("capital-loss", IntegerType(), True),
                                            StructField("hours-per-week", IntegerType(), True),
                                            StructField("native-country", StringType(), True),                                         
                                            StructField("income", StringType(), True)
                                            ])   

    df = (spark.getOrCreate().read
                  .format("csv")
                  .option("header", "true")
                  .schema(schema_census_income)
                  .load("./data/census-income/census-income.csv"))

    #df.printSchema()

#SUBSTITUIR "?" POR NONE E TRANSFORMAR STRING UPPERCASE
def Trans_Substituir_Interrogacao(df):
    names = ("workclass", "education", "marital-status", "occupation", "relashionship", "race", "sex", "native-country", "income")
    for c in names:
        df = df.withColumn(c, (F.when((F.col(c).contains("?")) , None)
                                .otherwise(F.upper(F.col(c)))
                              )
                          )
    return df
    
def Analise_Exploratoria(df):
    names = df.schema.names
    for c in names:
        df.groupBy(F.col(c)).agg(F.count(F.col(c))).orderBy(F.col(c).desc()).show()
        df.filter(F.col(c).contains("?")).groupBy(F.col(c)).agg(F.count(F.col(c))).orderBy(F.col(c).desc()).show()

#Analise_Exploratoria(df)

def P1_CI():
    print("Pergunta 1")
    (df.filter((F.col("income").contains(">50K")) & (F.col("workclass").isNotNull()))
       .groupBy(F.col("workclass"))
       .agg(F.sum(F.col("fnlwgt")).alias("People"))
       .orderBy(F.col("People").desc()).show())

df = Trans_Substituir_Interrogacao(df)
P1_CI()

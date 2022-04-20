from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
 
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
    
#SUBSTITUIR "?" POR NONE E TRANSFORMAR STRING UPPERCASE
def Trans_Var_Married(df):
    df = df.withColumn("marital_status_married", (F.when((F.col("marital-status").contains("MARRIED-CIV-SPOUSE")) |
                                                         (F.col("marital-status").contains("MARRIED-AF-SPOUSE")) |
                                                         (F.col("marital-status").contains("MARRIED-SPOUSE-ABSENT")) , "MARRIED")
                                                   .when((F.col("marital-status").isNull()) , None)
                                                   .otherwise("NOT MARRIED")
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

def P2_CI():
    print("Pergunta 2")
    (df.filter((F.col("race").isNotNull()))
       .groupBy(F.col("race"))
       .agg(F.round((F.sum(F.col("hours-per-week") * F.col("fnlwgt"))/F.sum(F.col("fnlwgt"))),2).alias("Avg_hours-per-week"))
       .orderBy(F.col("Avg_hours-per-week").desc()).show())

def P3_CI():
    print("Pergunta 3")

    Population = df.filter((F.col("sex").isNotNull())).agg(F.sum(F.col("fnlwgt"))).collect()[0][0]

    (df.filter((F.col("sex").isNotNull()))
       .groupBy(F.col("sex"))
       .agg(F.round((F.sum(F.col("fnlwgt"))/Population),2).alias("Prop_Sex"))
       .orderBy(F.col("Prop_Sex").desc()).show())

def P4_CI():
    print("Pergunta 4")

    Population = df.filter((F.col("sex").isNotNull())).agg(F.sum(F.col("fnlwgt"))).collect()[0][0]

    (df.filter((F.col("sex").isNotNull()))
       .groupBy(F.col("sex"))
       .agg(F.round((F.sum(F.col("fnlwgt"))/Population),2).alias("Prop_Sex"))
       .orderBy(F.col("Prop_Sex").desc()).show())

def P5_CI():
    print("Pergunta 5")

    df_aux1 = (df.filter((F.col("occupation").isNotNull()))
                 .groupBy(F.col("occupation"))
                 .agg(F.round((F.sum(F.col("hours-per-week") * F.col("fnlwgt"))/F.sum(F.col("fnlwgt"))),2).alias("Max_Avg_hours_per_week"))
                 .orderBy(F.col("Max_Avg_hours_per_week").desc()))

    df_aux2 = (df_aux1.agg(F.max("Max_Avg_hours_per_week").alias("Max")))

    df_result = (df_aux1.join(df_aux2,  (df_aux1.Max_Avg_hours_per_week ==  df_aux2.Max) 
                                        ,"left"))

    (df_result.filter(F.col('Max_Avg_hours_per_week') == F.col('Max'))
                    .select(F.col("occupation"), F.col("Max_Avg_hours_per_week"))).show(truncate=False)

def P6_CI():
    print("Pergunta 6")

    df_aux1 = (df.filter((F.col("education").isNotNull()) & (F.col("occupation").isNotNull()))
                 .groupBy(F.col("education-num"), F.col("education"), F.col("occupation"))
                 .agg(F.sum("fnlwgt").alias("People")))

    Part_A1 = Window.partitionBy(F.col("education")).orderBy(F.col("education"), F.col("People").desc())

    (df_aux1.withColumn("row",F.row_number().over(Part_A1))
                    .filter((F.col("row") == 1))
                    .select(F.col("education"), F.col("occupation"), F.col("People"))
                    .orderBy(F.col("education-num").desc())).show()

def P7_CI():
    print("Pergunta 7")

    df_aux1 = (df.filter((F.col("sex").isNotNull()) & (F.col("occupation").isNotNull()))
                 .groupBy(F.col("sex"), F.col("occupation"))
                 .agg(F.sum("fnlwgt").alias("People")))

    Part_A1 = Window.partitionBy(F.col("sex")).orderBy(F.col("sex"), F.col("People").desc())

    (df_aux1.withColumn("row",F.row_number().over(Part_A1))
                    .filter((F.col("row") == 1))
                    .select(F.col("sex"), F.col("occupation"), F.col("People"))
                    .orderBy(F.col("sex").desc())).show()

def P8_CI():
    print("Pergunta 8")

    df_aux1 = (df.filter((F.col("race").isNotNull()) & (F.col("education").isNotNull()))
                 .select(F.col("education-num"), F.col("education"), F.col("race"))
                 .distinct())

    Part_A1 = Window.partitionBy(F.col("race")).orderBy(F.col("race"), F.col("education-num").desc())

    (df_aux1.withColumn("row",F.row_number().over(Part_A1))
                    .filter((F.col("row") == 1))
                    .select(F.col("race"), F.col("education"))
                    .orderBy(F.col("education-num").desc())).show()

def P9_CI():
    print("Pergunta 9")

    df_aux1 = (df.filter((F.col("education").isNotNull()) & (F.col("sex").isNotNull()) & (F.col("race").isNotNull()) & (F.col("workclass").contains("SELF-EMP-INC")))
                 .groupBy(F.col("education"), F.col("sex"), F.col("race"))
                 .agg(F.sum("fnlwgt").alias("People")))

    #df_aux1.orderBy(F.col("People").desc()).show()

    df_aux2 = (df_aux1.agg(F.max("People").alias("Max")))

    df_result = (df_aux1.join(df_aux2,  (df_aux1.People ==  df_aux2.Max) 
                                        ,"left"))

    (df_result.filter(F.col('People') == F.col('Max'))
                    .select(F.col("education"), F.col("sex"), F.col("race"), F.col("People"))).show(truncate=False)

def P10_CI():
    print("Pergunta 10")

    People_Married = df.filter((F.col("marital_status_married").isNotNull()) & (F.col("marital_status_married") == "MARRIED")).agg(F.sum(F.col("fnlwgt"))).collect()[0][0]
    People_NotMarried = df.filter((F.col("marital_status_married").isNotNull()) & (F.col("marital_status_married") == "NOT MARRIED")).agg(F.sum(F.col("fnlwgt"))).collect()[0][0]

    print(round((People_Married/People_NotMarried),2))

def P11_CI():
    print("Pergunta 11")

    df_aux1 = (df.filter((F.col("race").isNotNull()) & (F.col("marital_status_married") == "NOT MARRIED"))
                 .groupBy(F.col("race"))
                 .agg(F.sum("fnlwgt").alias("People")))

    #df_aux1.orderBy(F.col("People").desc()).show()

    df_aux2 = (df_aux1.agg(F.max("People").alias("Max")))

    df_result = (df_aux1.join(df_aux2,  (df_aux1.People ==  df_aux2.Max) 
                                        ,"left"))

    (df_result.filter(F.col('People') == F.col('Max'))
                    .select(F.col("race"), F.col("People"))).show(truncate=False)

def P12_CI():
    print("Pergunta 12")

    df_aux1 = (df.filter((F.col("marital_status_married").isNotNull()) & (F.col("income").isNotNull()))
                 .groupBy(F.col("marital_status_married"), F.col("income"))
                 .agg(F.sum("fnlwgt").alias("People")))

    #df_aux1.orderBy(F.col("marital_status_married"), F.col("People").desc()).show()

    Part_A1 = Window.partitionBy(F.col("marital_status_married")).orderBy(F.col("marital_status_married"), F.col("People").desc())

    (df_aux1.withColumn("row",F.row_number().over(Part_A1))
                    .filter((F.col("row") == 1))
                    .select(F.col("marital_status_married"), F.col("income"), F.col("People"))
                    .orderBy(F.col("marital_status_married").desc())).show()

def P13_CI():
    print("Pergunta 13")

    df_aux1 = (df.filter((F.col("sex").isNotNull()) & (F.col("income").isNotNull()))
                 .groupBy(F.col("sex"), F.col("income"))
                 .agg(F.sum("fnlwgt").alias("People")))

    #df_aux1.orderBy(F.col("sex"), F.col("People").desc()).show()

    Part_A1 = Window.partitionBy(F.col("sex")).orderBy(F.col("sex"), F.col("People").desc())

    (df_aux1.withColumn("row",F.row_number().over(Part_A1))
                    .filter((F.col("row") == 1))
                    .select(F.col("sex"), F.col("income"), F.col("People"))
                    .orderBy(F.col("sex").desc())).show()

df = Trans_Substituir_Interrogacao(df)
df = Trans_Var_Married(df)
#P1_CI()
#P2_CI()
#P3_CI()
#P4_CI()
#P5_CI()
#P6_CI()
#P7_CI()
#P8_CI()
#P10_CI()
#P11_CI()
#P12_CI()
P13_CI()
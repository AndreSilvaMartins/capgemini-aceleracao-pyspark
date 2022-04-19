from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	schema_communities_crime = StructType([
											StructField("state", IntegerType(), True),
											StructField("county", IntegerType(), True),
											StructField("community", IntegerType(), True),
											StructField("communityname", StringType(), True),
											StructField("fold", IntegerType(), True),
											StructField("population", FloatType(), True),
											StructField("householdsize", FloatType(), True),
											StructField("racepctblack", FloatType(), True),
											StructField("racePctWhite", FloatType(), True),
											StructField("racePctAsian", FloatType(), True),
											StructField("racePctHisp", FloatType(), True),
											StructField("agePct12t21", FloatType(), True),
											StructField("agePct12t29", FloatType(), True),
											StructField("agePct16t24", FloatType(), True),
											StructField("agePct65up", FloatType(), True),
											StructField("numbUrban", FloatType(), True),
											StructField("pctUrban", FloatType(), True),
											StructField("medIncome", FloatType(), True),
											StructField("pctWWage", FloatType(), True),
											StructField("pctWFarmSelf", FloatType(), True),
											StructField("pctWInvInc", FloatType(), True),
											StructField("pctWSocSec", FloatType(), True),
											StructField("pctWPubAsst", FloatType(), True),
											StructField("pctWRetire", FloatType(), True),
											StructField("medFamInc", FloatType(), True),
											StructField("perCapInc", FloatType(), True),
											StructField("whitePerCap", FloatType(), True),
											StructField("blackPerCap", FloatType(), True),
											StructField("indianPerCap", FloatType(), True),
											StructField("AsianPerCap", FloatType(), True),
											StructField("OtherPerCap", FloatType(), True),
											StructField("HispPerCap", FloatType(), True),
											StructField("NumUnderPov", FloatType(), True),
											StructField("PctPopUnderPov", FloatType(), True),
											StructField("PctLess9thGrade", FloatType(), True),
											StructField("PctNotHSGrad", FloatType(), True),
											StructField("PctBSorMore", FloatType(), True),
											StructField("PctUnemployed", FloatType(), True),
											StructField("PctEmploy", FloatType(), True),
											StructField("PctEmplManu", FloatType(), True),
											StructField("PctEmplProfServ", FloatType(), True),
											StructField("PctOccupManu", FloatType(), True),
											StructField("PctOccupMgmtProf", FloatType(), True),
											StructField("MalePctDivorce", FloatType(), True),
											StructField("MalePctNevMarr", FloatType(), True),
											StructField("FemalePctDiv", FloatType(), True),
											StructField("TotalPctDiv", FloatType(), True),
											StructField("PersPerFam", FloatType(), True),
											StructField("PctFam2Par", FloatType(), True),
											StructField("PctKids2Par", FloatType(), True),
											StructField("PctYoungKids2Par", FloatType(), True),
											StructField("PctTeen2Par", FloatType(), True),
											StructField("PctWorkMomYoungKids", FloatType(), True),
											StructField("PctWorkMom", FloatType(), True),
											StructField("NumIlleg", FloatType(), True),
											StructField("PctIlleg", FloatType(), True),
											StructField("NumImmig", FloatType(), True),
											StructField("PctImmigRecent", FloatType(), True),
											StructField("PctImmigRec5", FloatType(), True),
											StructField("PctImmigRec8", FloatType(), True),
											StructField("PctImmigRec10", FloatType(), True),
											StructField("PctRecentImmig", FloatType(), True),
											StructField("PctRecImmig5", FloatType(), True),
											StructField("PctRecImmig8", FloatType(), True),
											StructField("PctRecImmig10", FloatType(), True),
											StructField("PctSpeakEnglOnly", FloatType(), True),
											StructField("PctNotSpeakEnglWell", FloatType(), True),
											StructField("PctLargHouseFam", FloatType(), True),
											StructField("PctLargHouseOccup", FloatType(), True),
											StructField("PersPerOccupHous", FloatType(), True),
											StructField("PersPerOwnOccHous", FloatType(), True),
											StructField("PersPerRentOccHous", FloatType(), True),
											StructField("PctPersOwnOccup", FloatType(), True),
											StructField("PctPersDenseHous", FloatType(), True),
											StructField("PctHousLess3BR", FloatType(), True),
											StructField("MedNumBR", FloatType(), True),
											StructField("HousVacant", FloatType(), True),
											StructField("PctHousOccup", FloatType(), True),
											StructField("PctHousOwnOcc", FloatType(), True),
											StructField("PctVacantBoarded", FloatType(), True),
											StructField("PctVacMore6Mos", FloatType(), True),
											StructField("MedYrHousBuilt", FloatType(), True),
											StructField("PctHousNoPhone", FloatType(), True),
											StructField("PctWOFullPlumb", FloatType(), True),
											StructField("OwnOccLowQuart", FloatType(), True),
											StructField("OwnOccMedVal", FloatType(), True),
											StructField("OwnOccHiQuart", FloatType(), True),
											StructField("RentLowQ", FloatType(), True),
											StructField("RentMedian", FloatType(), True),
											StructField("RentHighQ", FloatType(), True),
											StructField("MedRent", FloatType(), True),
											StructField("MedRentPctHousInc", FloatType(), True),
											StructField("MedOwnCostPctInc", FloatType(), True),
											StructField("MedOwnCostPctIncNoMtg", FloatType(), True),
											StructField("NumInShelters", FloatType(), True),
											StructField("NumStreet", FloatType(), True),
											StructField("PctForeignBorn", FloatType(), True),
											StructField("PctBornSameState", FloatType(), True),
											StructField("PctSameHouse85", FloatType(), True),
											StructField("PctSameCity85", FloatType(), True),
											StructField("PctSameState85", FloatType(), True),
											StructField("LemasSwornFT", FloatType(), True),
											StructField("LemasSwFTPerPop", FloatType(), True),
											StructField("LemasSwFTFieldOps", FloatType(), True),
											StructField("LemasSwFTFieldPerPop", FloatType(), True),
											StructField("LemasTotalReq", FloatType(), True),
											StructField("LemasTotReqPerPop", FloatType(), True),
											StructField("PolicReqPerOffic", FloatType(), True),
											StructField("PolicPerPop", FloatType(), True),
											StructField("RacialMatchCommPol", FloatType(), True),
											StructField("PctPolicWhite", FloatType(), True),
											StructField("PctPolicBlack", FloatType(), True),
											StructField("PctPolicHisp", FloatType(), True),
											StructField("PctPolicAsian", FloatType(), True),
											StructField("PctPolicMinor", FloatType(), True),
											StructField("OfficAssgnDrugUnits", FloatType(), True),
											StructField("NumKindsDrugsSeiz", FloatType(), True),
											StructField("PolicAveOTWorked", FloatType(), True),
											StructField("LandArea", FloatType(), True),
											StructField("PopDens", FloatType(), True),
											StructField("PctUsePubTrans", FloatType(), True),
											StructField("PolicCars", FloatType(), True),
											StructField("PolicOperBudg", FloatType(), True),
											StructField("LemasPctPolicOnPatr", FloatType(), True),
											StructField("LemasGangUnitDeploy", FloatType(), True),
											StructField("LemasPctOfficDrugUn", FloatType(), True),
											StructField("PolicBudgPerPop", FloatType(), True),
											StructField("ViolentCrimesPerPop", FloatType(), True)
											])

	df = (spark.getOrCreate().read
		          						 .format("csv")
		          						 .option("header", "true")
		          						 #.schema(schema_communities_crime)
		          						 .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))

#SUBSTITUIR "?" POR NONE
def Trans_Substituir_Interrogacao(df):
	names = df.schema.names
	for c in names:
		df = df.withColumn(c, (F.when((F.col(c).contains("?")) , None)
														.otherwise(F.col(c))
										)
													)

		if (c != "communityname") & (c in ("state", "county", "community", "fold")):
			df = df.withColumn(c, (F.col(c).cast("int")))	
		elif (c != "communityname"):
			df = df.withColumn(c, (F.col(c).cast("float")))

	return df

def P1_CC():
	print("Pergunta 1 - Qual comunidade tem maior orçamento policial?")

	df_communityname = (df.filter(F.col('PolicOperBudg') > 0)
							.groupBy(F.col('state'), F.col('communityname'))
							.agg(F.sum("PolicOperBudg").alias("Max_Budget")))

	df_max = (df_communityname.agg(F.max("Max_Budget").alias("Max")))

	df_result = (df_communityname.join(df_max, 
										(df_communityname.Max_Budget ==  df_max.Max) 
										,"left"))

	print((df_result.filter(F.col('Max_Budget') == F.col('Max'))
					.select(F.col("State"), F.col("communityname"), F.col("Max_Budget"))).show(truncate=False))

def P2_CC():
	print("Pergunta 2 - Qual comunidade tem maior número de crimes violentos?")
	
	df_communityname = (df.filter(F.col('ViolentCrimesPerPop') > 0)
							.groupBy(F.col('state'), F.col('communityname'))
							.agg(F.max("ViolentCrimesPerPop").alias("Max_Violence")))

	df_max = (df_communityname.agg(F.max("Max_Violence").alias("Max")))

	df_result = (df_communityname.join(df_max, 
										(df_communityname.Max_Violence ==  df_max.Max) 
										,"left"))

	print((df_result.filter(F.col('Max_Violence') == F.col('Max'))
					.select(F.col("State"), F.col("communityname"), F.col("Max_Violence"))).show(truncate=False))

def P3_CC():
	print("Pergunta 3 - Qual comunidade tem maior população?")
	
	df_communityname = (df.filter(F.col('population') > 0)
							.groupBy(F.col('state'), F.col('communityname'))
							.agg(F.max("population").alias("Max_population")))

	df_max = (df_communityname.agg(F.max("Max_population").alias("Max")))

	df_result = (df_communityname.join(df_max, 
										(df_communityname.Max_population ==  df_max.Max) 
										,"left"))

	print((df_result.filter(F.col('Max_population') == F.col('Max'))
					.select(F.col("State"), F.col("communityname"), F.col("Max_population"))).show(truncate=False))

def P4_CC():
	print("Pergunta 4 - Qual comunidade tem maior população negra?")
	
	df_communityname = (df.filter((F.col('population') > 0) & (F.col('racepctblack') >= 0))
							.groupBy(F.col('state'), F.col('communityname'))
							.agg(F.max((F.col("population") * F.col("racepctblack"))).alias("Max_black_population")))

	df_max = (df_communityname.agg(F.max("Max_black_population").alias("Max")))

	df_result = (df_communityname.join(df_max, 
										(df_communityname.Max_black_population ==  df_max.Max) 
										,"left"))

	print((df_result.filter(F.col('Max_black_population') == F.col('Max'))
					.select(F.col("State"), F.col("communityname"), F.col("Max_black_population"))).show(truncate=False))

df = Trans_Substituir_Interrogacao(df)
#P1_CC()
#P2_CC()
#P3_CC()
P4_CC()


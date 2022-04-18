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
											StructField("population", IntegerType(), True),
											StructField("householdsize", IntegerType(), True),
											StructField("racepctblack", IntegerType(), True),
											StructField("racePctWhite", IntegerType(), True),
											StructField("racePctAsian", IntegerType(), True),
											StructField("racePctHisp", IntegerType(), True),
											StructField("agePct12t21", IntegerType(), True),
											StructField("agePct12t29", IntegerType(), True),
											StructField("agePct16t24", IntegerType(), True),
											StructField("agePct65up", IntegerType(), True),
											StructField("numbUrban", IntegerType(), True),
											StructField("pctUrban", IntegerType(), True),
											StructField("medIncome", IntegerType(), True),
											StructField("pctWWage", IntegerType(), True),
											StructField("pctWFarmSelf", IntegerType(), True),
											StructField("pctWInvInc", IntegerType(), True),
											StructField("pctWSocSec", IntegerType(), True),
											StructField("pctWPubAsst", IntegerType(), True),
											StructField("pctWRetire", IntegerType(), True),
											StructField("medFamInc", IntegerType(), True),
											StructField("perCapInc", IntegerType(), True),
											StructField("whitePerCap", IntegerType(), True),
											StructField("blackPerCap", IntegerType(), True),
											StructField("indianPerCap", IntegerType(), True),
											StructField("AsianPerCap", IntegerType(), True),
											StructField("OtherPerCap", IntegerType(), True),
											StructField("HispPerCap", IntegerType(), True),
											StructField("NumUnderPov", IntegerType(), True),
											StructField("PctPopUnderPov", IntegerType(), True),
											StructField("PctLess9thGrade", IntegerType(), True),
											StructField("PctNotHSGrad", IntegerType(), True),
											StructField("PctBSorMore", IntegerType(), True),
											StructField("PctUnemployed", IntegerType(), True),
											StructField("PctEmploy", IntegerType(), True),
											StructField("PctEmplManu", IntegerType(), True),
											StructField("PctEmplProfServ", IntegerType(), True),
											StructField("PctOccupManu", IntegerType(), True),
											StructField("PctOccupMgmtProf", IntegerType(), True),
											StructField("MalePctDivorce", IntegerType(), True),
											StructField("MalePctNevMarr", IntegerType(), True),
											StructField("FemalePctDiv", IntegerType(), True),
											StructField("TotalPctDiv", IntegerType(), True),
											StructField("PersPerFam", IntegerType(), True),
											StructField("PctFam2Par", IntegerType(), True),
											StructField("PctKids2Par", IntegerType(), True),
											StructField("PctYoungKids2Par", IntegerType(), True),
											StructField("PctTeen2Par", IntegerType(), True),
											StructField("PctWorkMomYoungKids", IntegerType(), True),
											StructField("PctWorkMom", IntegerType(), True),
											StructField("NumIlleg", IntegerType(), True),
											StructField("PctIlleg", IntegerType(), True),
											StructField("NumImmig", IntegerType(), True),
											StructField("PctImmigRecent", IntegerType(), True),
											StructField("PctImmigRec5", IntegerType(), True),
											StructField("PctImmigRec8", IntegerType(), True),
											StructField("PctImmigRec10", IntegerType(), True),
											StructField("PctRecentImmig", IntegerType(), True),
											StructField("PctRecImmig5", IntegerType(), True),
											StructField("PctRecImmig8", IntegerType(), True),
											StructField("PctRecImmig10", IntegerType(), True),
											StructField("PctSpeakEnglOnly", IntegerType(), True),
											StructField("PctNotSpeakEnglWell", IntegerType(), True),
											StructField("PctLargHouseFam", IntegerType(), True),
											StructField("PctLargHouseOccup", IntegerType(), True),
											StructField("PersPerOccupHous", IntegerType(), True),
											StructField("PersPerOwnOccHous", IntegerType(), True),
											StructField("PersPerRentOccHous", IntegerType(), True),
											StructField("PctPersOwnOccup", IntegerType(), True),
											StructField("PctPersDenseHous", IntegerType(), True),
											StructField("PctHousLess3BR", IntegerType(), True),
											StructField("MedNumBR", IntegerType(), True),
											StructField("HousVacant", IntegerType(), True),
											StructField("PctHousOccup", IntegerType(), True),
											StructField("PctHousOwnOcc", IntegerType(), True),
											StructField("PctVacantBoarded", IntegerType(), True),
											StructField("PctVacMore6Mos", IntegerType(), True),
											StructField("MedYrHousBuilt", IntegerType(), True),
											StructField("PctHousNoPhone", IntegerType(), True),
											StructField("PctWOFullPlumb", IntegerType(), True),
											StructField("OwnOccLowQuart", IntegerType(), True),
											StructField("OwnOccMedVal", IntegerType(), True),
											StructField("OwnOccHiQuart", IntegerType(), True),
											StructField("RentLowQ", IntegerType(), True),
											StructField("RentMedian", IntegerType(), True),
											StructField("RentHighQ", IntegerType(), True),
											StructField("MedRent", IntegerType(), True),
											StructField("MedRentPctHousInc", IntegerType(), True),
											StructField("MedOwnCostPctInc", IntegerType(), True),
											StructField("MedOwnCostPctIncNoMtg", IntegerType(), True),
											StructField("NumInShelters", IntegerType(), True),
											StructField("NumStreet", IntegerType(), True),
											StructField("PctForeignBorn", IntegerType(), True),
											StructField("PctBornSameState", IntegerType(), True),
											StructField("PctSameHouse85", IntegerType(), True),
											StructField("PctSameCity85", IntegerType(), True),
											StructField("PctSameState85", IntegerType(), True),
											StructField("LemasSwornFT", IntegerType(), True),
											StructField("LemasSwFTPerPop", IntegerType(), True),
											StructField("LemasSwFTFieldOps", IntegerType(), True),
											StructField("LemasSwFTFieldPerPop", IntegerType(), True),
											StructField("LemasTotalReq", IntegerType(), True),
											StructField("LemasTotReqPerPop", IntegerType(), True),
											StructField("PolicReqPerOffic", IntegerType(), True),
											StructField("PolicPerPop", IntegerType(), True),
											StructField("RacialMatchCommPol", IntegerType(), True),
											StructField("PctPolicWhite", IntegerType(), True),
											StructField("PctPolicBlack", IntegerType(), True),
											StructField("PctPolicHisp", IntegerType(), True),
											StructField("PctPolicAsian", IntegerType(), True),
											StructField("PctPolicMinor", IntegerType(), True),
											StructField("OfficAssgnDrugUnits", IntegerType(), True),
											StructField("NumKindsDrugsSeiz", IntegerType(), True),
											StructField("PolicAveOTWorked", IntegerType(), True),
											StructField("LandArea", IntegerType(), True),
											StructField("PopDens", IntegerType(), True),
											StructField("PctUsePubTrans", IntegerType(), True),
											StructField("PolicCars", IntegerType(), True),
											StructField("PolicOperBudg", IntegerType(), True),
											StructField("LemasPctPolicOnPatr", IntegerType(), True),
											StructField("LemasGangUnitDeploy", IntegerType(), True),
											StructField("LemasPctOfficDrugUn", IntegerType(), True),
											StructField("PolicBudgPerPop", IntegerType(), True),
											StructField("ViolentCrimesPerPop", IntegerType(), True)
											])

	emptyRDD = spark.getOrCreate().sparkContext.emptyRDD()
	df = spark.getOrCreate().createDataFrame(emptyRDD,schema_communities_crime)

	df_noTransform = (spark.getOrCreate().read
		          						 .format("csv")
		          						 .option("header", "true")
		          						#.schema(schema_communities_crime)
		          						 .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	print(df_noTransform.show())

	#SUBSTITUIR "?" POR NONE
	def Trans_Substituir_Interrogacao(df_noTransform):
		names = df_noTransform.schema.names
		for c in names:
			df_noTransform = df_noTransform.withColumn(c, (F.when((F.col(c) == "?") , None)
															.otherwise(F.col(c))
											)
										)

		return df_noTransform

	df = df.union(Trans_Substituir_Interrogacao(df_noTransform))
	df.printSchema()
	print(df.show())


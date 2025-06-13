from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, DoubleType, IntegerType, DateType, LongType

schema_UNHCR_asylumapplications = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Procedure Type", StringType(), True),
    StructField("Application type", StringType(), True),
    StructField("Decision level", StringType(), True),
    StructField("App_pc", StringType(), True),
    StructField("Applied", IntegerType(), True)
])

schema_UNHCR_asylumdecisions = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Procedure Type", StringType(), True),
    StructField("Dec level", StringType(), True),
    StructField("Dec pc", StringType(), True),
    StructField("Dec recognized", IntegerType(), True),
    StructField("dec other", IntegerType(), True),
    StructField("dec rejected", IntegerType(), True),
    StructField("dec closed", IntegerType(), True),
    StructField("dec total", IntegerType(), True)
])

schema_UNHCR_idpidmc = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_idpreturnees = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_nowcasting = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Month", StringType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Refugees", IntegerType(), True),
    StructField("Asylum Seekers", IntegerType(), True),
    StructField("Source", StringType(), True)
])

schema_UNHCR_palestinerefugees = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Population type", StringType(), True),
    StructField("Female 0-4", IntegerType(), True),
    StructField("Female 5-11", IntegerType(), True),
    StructField("Female 12-17", IntegerType(), True),
    StructField("Female 18-59", IntegerType(), True),
    StructField("Female 60+", IntegerType(), True),
    StructField("Female Total", IntegerType(), True),
    StructField("Male 0-4", IntegerType(), True),
    StructField("Male 5-11", IntegerType(), True),
    StructField("Male 12-17", IntegerType(), True),
    StructField("Male 18-59", IntegerType(), True),
    StructField("Male 60+", IntegerType(), True),
    StructField("Male Total", IntegerType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_population = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Female 0 - 4", IntegerType(), True),
    StructField("Female 5 - 11", IntegerType(), True),
    StructField("Female 12 - 17", IntegerType(), True),
    StructField("Female 18 - 59", IntegerType(), True),
    StructField("Female 60+", IntegerType(), True),
    StructField("Female Other", IntegerType(), True),
    StructField("Female Total", IntegerType(), True),
    StructField("Male 0 - 4", IntegerType(), True),
    StructField("Male 5 - 11", IntegerType(), True),
    StructField("Male 12 - 17", IntegerType(), True),
    StructField("Male 18 - 59", IntegerType(), True),
    StructField("Male 60+", IntegerType(), True),
    StructField("Male Other", IntegerType(), True),
    StructField("Male Total", IntegerType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_refugeenaturalization = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_refugeereturnees = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_resettlementdepartures = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Resettlement", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Country of Resettlement ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_resettlementneeds = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True)
])

schema_UNHCR_resettlementsubmissionrequests = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Asylum", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Resettlement", StringType(), True),
    StructField("Country of Asylum ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Country of Resettlement ISO", StringType(), True),
    StructField("Cases", IntegerType(), True),
    StructField("Persons", IntegerType(), True),
    StructField("Month", StringType(), True)

])

schema_UNHCR_resettlementsubmissions = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Country of Resettlement", StringType(), True),
    StructField("Country of Origin", StringType(), True),
    StructField("Country of Resettlement ISO", StringType(), True),
    StructField("Country of Origin ISO", StringType(), True),
    StructField("Total", IntegerType(), True),
    StructField("Month", StringType(), True)

])
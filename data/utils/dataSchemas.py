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
from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrAsylumDecisions(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="asylumdecisions", id_column_name="id_asylumdecisions")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_asylumdecisions",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Asylum ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit("")),
                coalesce(col("Procedure Type"), lit("")),
                coalesce(col("Dec level"), lit("")),
                coalesce(col("Dec pc"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrAsylumDecisions().execute()
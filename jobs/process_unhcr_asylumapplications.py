from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrAsylumApplications(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="asylumapplications", id_column_name="id_asylumapplications")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_asylumapplications",
            concat_ws(
                "_",
                coalesce(col("Country of Asylum ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit("")),
                coalesce(col("Procedure Type"), lit("")),
                coalesce(col("Application type"), lit("")),
                coalesce(col("Decision level"), lit("")),
                coalesce(col("App_pc"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrAsylumApplications().execute()
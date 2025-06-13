from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrPopulation(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="population", id_column_name="id_population")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_population",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Asylum ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrPopulation().execute()
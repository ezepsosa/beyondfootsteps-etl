from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrRefugeereturnees(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="refugeereturnees", id_column_name="id_refugeereturnees")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_refugeereturnees",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Origin ISO"), lit("")),
                coalesce(col("Country of Asylum ISO"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrRefugeereturnees().execute()
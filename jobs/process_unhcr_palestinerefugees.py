from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrPalestineRefugees(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="palestinerefugees", id_column_name="id_palestinerefugees")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_palestinerefugees",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Asylum ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit("")),
                coalesce(col("Population type"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrPalestineRefugees().execute()
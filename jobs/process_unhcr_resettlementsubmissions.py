from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrResettlementSubmissions(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="resettlementsubmissions", id_column_name="id_resettlementsubmissions")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_resettlementsubmissions",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Resettlement ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit(""))
            )
        )
if __name__ == "__main__":
    ProcessUnhcrResettlementSubmissions().execute()
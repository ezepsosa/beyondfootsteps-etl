from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessUnhcrResettlementSubmissionsRequests(SilverProcessor):
    def __init__(self):
        super().__init__(origin="UNHCR", entity="resettlementsubmissionrequests", id_column_name="id_resettlementsubmissionrequests")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "id_resettlementsubmissionrequests",
            concat_ws(
                "_",
                coalesce(col("Year"), lit("")),
                coalesce(col("Country of Asylum ISO"), lit("")),
                coalesce(col("Country of Origin ISO"), lit("")),
                coalesce(col("Country of Resettlement ISO"), lit(""))

            )
        )
if __name__ == "__main__":
    ProcessUnhcrResettlementSubmissionsRequests().execute()
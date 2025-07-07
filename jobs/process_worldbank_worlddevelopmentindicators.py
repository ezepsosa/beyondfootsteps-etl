from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessWorldBankWorldDevelopmentIndicators(SilverProcessor):
    def __init__(self):
        super().__init__(origin="WorldBank", entity="worlddevelopmentindicators", id_column_name="Country Code")
        
    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumnRenamed("1990 [YR1990]", "1990") \
            .withColumnRenamed("2000 [YR2000]", "2000") \
            .withColumnRenamed("2015 [YR2015]", "2015") \
            .withColumnRenamed("2016 [YR2016]", "2016") \
            .withColumnRenamed("2017 [YR2017]", "2017") \
            .withColumnRenamed("2018 [YR2018]", "2018") \
            .withColumnRenamed("2019 [YR2019]", "2019") \
            .withColumnRenamed("2020 [YR2020]", "2020") \
            .withColumnRenamed("2021 [YR2021]", "2021") \
            .withColumnRenamed("2022 [YR2022]", "2022") \
            .withColumnRenamed("2023 [YR2023]", "2023") \
            .withColumnRenamed("2024 [YR2024]", "2024")


if __name__ == "__main__":
    ProcessWorldBankWorldDevelopmentIndicators().execute()
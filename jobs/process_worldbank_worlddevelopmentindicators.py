from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessWorldBankWorldDevelopmentIndicators(SilverProcessor):

    def __init__(self):
        super().__init__(origin="WorldBank",
                         entity="worlddevelopmentindicators",
                         id_column_name="Country Code")

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df.melt(ids=["Country Code", "Country Name", "intakeDate"],
                       values=[
                           "2000 [YR2000]", "2015 [YR2015]", "2016 [YR2016]",
                           "2017 [YR2017]", "2018 [YR2018]", "2019 [YR2019]",
                           "2020 [YR2020]", "2021 [YR2021]", "2022 [YR2022]",
                           "2023 [YR2023]", "2024 [YR2024]"
                       ], valueColumnName="year", variableColumnName="population")


if __name__ == "__main__":
    ProcessWorldBankWorldDevelopmentIndicators().execute()

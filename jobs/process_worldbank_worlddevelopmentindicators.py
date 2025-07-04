from typing import override

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, coalesce, lit

from sparklibs.job import SilverProcessor


class ProcessWorldBankWorldDevelopmentIndicators(SilverProcessor):
    def __init__(self):
        super().__init__(origin="WorldBank", entity="worlddevelopmentindicators", id_column_name="Country Code")

if __name__ == "__main__":
    ProcessWorldBankWorldDevelopmentIndicators().execute()
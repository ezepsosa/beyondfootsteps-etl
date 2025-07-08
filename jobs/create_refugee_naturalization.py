from argparse import Namespace

from pyspark.sql.window import Window

from pyspark.sql.functions import lag, col, try_divide

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):

        # DATAFRAMES LOAD

        df_naturalization = self._get_last_version_from_silver(configuration, origin="UNHCR", entity="refugeenaturalization").select("id_refugeenaturalization", "year", "country_of_asylum", "country_of_asylum_iso", "country_of_origin", "country_of_origin_iso", "total", "intake_date")

        window_spec = Window.partitionBy("country_of_asylum_iso", "country_of_origin_iso").orderBy("year")

        df_naturalization = df_naturalization.withColumn("previous_total", lag("total").over(window_spec)) \
            .withColumn("naturalization_change", try_divide((col("total") - col("previous_total")),col("previous_total"))) \
            .drop(col("previous_total"))

        self._save_in_database(df_naturalization, "naturalization_kpi", configuration)

        
if __name__ == '__main__':
    ResettlementSummaryJob().execute(),

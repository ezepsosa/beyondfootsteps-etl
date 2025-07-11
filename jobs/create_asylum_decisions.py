from argparse import Namespace

from pyspark.sql.functions import col, try_divide

from sparklibs.job import GoldJob, Configuration


class AsylumDecisionsJobs(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "asylum_decisions_kpi"

        # DATAFRAMES LOAD

        df_asylumdecisions = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="asylumdecisions"
        )

        # TABLES JOIN

        df_asylumdecisions = df_asylumdecisions.withColumn(
            "acceptance_rate", try_divide(col("dec_recognized"), col("dec_total"))
        ).select(
            col("id_asylumdecisions"),
            col("year"),
            col("country_of_origin"),
            col("country_of_origin_iso"),
            col("country_of_asylum"),
            col("country_of_asylum_iso"),
            col("dec_recognized"),
            col("dec_other"),
            col("dec_rejected"),
            col("dec_closed"),
            col("dec_total"),
            col("acceptance_rate"),
            col("intake_date"),
        )

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_asylumdecisions.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_asylumdecisions, kpi_name, configuration)


if __name__ == "__main__":
    (AsylumDecisionsJobs().execute(),)

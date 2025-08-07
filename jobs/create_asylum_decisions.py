from argparse import Namespace

from pyspark.sql.functions import (
    col,
    try_divide,
    sum as _sum,
    max as _max,
    when,
    lit,
    concat_ws,
)

from sparklibs.job import GoldJob, Configuration


class AsylumDecisionsJobs(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "asylum_decisions_kpi"

        df_asylumdecisions = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="asylumdecisions"
        )

        df_decisions_agg = df_asylumdecisions.groupBy(
            "year",
            "country_of_origin",
            "country_of_origin_iso",
            "country_of_asylum",
            "country_of_asylum_iso",
        ).agg(
            _sum("dec_recognized").alias("dec_recognized"),
            _sum("dec_other").alias("dec_other"),
            _sum("dec_rejected").alias("dec_rejected"),
            _sum("dec_closed").alias("dec_closed"),
            _sum("dec_total").alias("dec_total"),
            _max(when(col("dec_pc") == "C", lit(True)).otherwise(lit(False))).alias(
                "dec_pc"
            ),
        )

        df_asylum_decisions = df_decisions_agg.withColumn(
            "acceptance_rate", try_divide(col("dec_recognized"), col("dec_total"))
        ).withColumn(
            "id",
            concat_ws(
                "_",
                col("year"),
                col("country_of_origin_iso"),
                col("country_of_asylum_iso"),
            ),
        )

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"
        df_asylum_decisions.write.parquet(output_directory, mode="overwrite")
        self._save_in_database(df_asylum_decisions, kpi_name, configuration)


if __name__ == "__main__":
    AsylumDecisionsJobs().execute()

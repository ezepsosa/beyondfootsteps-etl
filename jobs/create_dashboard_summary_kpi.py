from argparse import Namespace

from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, concat_ws
from sparklibs.job import GoldJob, Configuration


class DashboardSummaryJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "dashboard_summary_kpi"

        # DATAFRAMES LOAD
        df_asylum_decisions_kpi = self._get_last_version_from_gold(
            configuration, entity="asylum_decisions_kpi"
        )
        df_asylum_requests_kpi = self._get_last_version_from_gold(
            configuration, entity="asylum_requests_kpi"
        )
        df_idp_displacement_kpi = self._get_last_version_from_gold(
            configuration, entity="idp_displacement_kpi"
        )
        df_idp_returnees_kpi = self._get_last_version_from_gold(
            configuration, entity="idp_returnees_kpi"
        )
        df_refugee_naturalization_kpi = self._get_last_version_from_gold(
            configuration, entity="refugee_naturalization_kpi"
        )
        df_resettlements_summary_kpi = self._get_last_version_from_gold(
            configuration, entity="resettlements_summary_kpi"
        )

        # RENAME AND SELECT STANDARDIZED FIELDS
        df_asylum_requests = df_asylum_requests_kpi.groupBy(
            "year", col("country_of_asylum_iso").alias("country_iso")
        ).agg(
            spark_sum("applied").alias("total_applied"),
            spark_avg("applied_per_100k").alias("applied_per_100k"),
        )

        df_asylum_decisions = df_asylum_decisions_kpi.groupBy(
            "year", col("country_of_asylum_iso").alias("country_iso")
        ).agg(spark_avg("acceptance_rate").alias("acceptance_rate"))

        df_idp_displacement = df_idp_displacement_kpi.groupBy(
            "year", col("country_of_origin_iso").alias("country_iso")
        ).agg(
            spark_sum("total").alias("internal_displacement_total"),
            spark_avg("displacement_rate_per_100k").alias("displacement_rate_per_100k"),
        )

        df_idp_returnees = df_idp_returnees_kpi.groupBy(
            "year", col("country_of_origin_iso").alias("country_iso")
        ).agg(
            spark_sum("idp_returnees").alias("idp_returnees"),
            spark_sum("refugees_returnees").alias("refugees_returnees"),
        )

        df_naturalization = df_refugee_naturalization_kpi.groupBy(
            "year", col("country_of_asylum_iso").alias("country_iso")
        ).agg(
            spark_sum("total").alias("naturalizations_total"),
            spark_avg("naturalization_change").alias("naturalization_change"),
        )

        df_resettlement = df_resettlements_summary_kpi.groupBy(
            "year", col("country_of_asylum_iso").alias("country_iso")
        ).agg(
            spark_sum("persons").alias("resettlement_requests"),
            spark_sum("departures_total").alias("resettlement_departures"),
            spark_sum("submissions_total").alias("resettlement_submissions"),
            spark_sum("total_needs").alias("resettlement_needs"),
            spark_avg("resettlement_gap").alias("resettlement_gap"),
            spark_avg("coverage_rate").alias("coverage_rate"),
            spark_avg("request_vs_needs_ratio").alias("request_vs_needs_ratio"),
            spark_avg("submissions_efficiency").alias("submissions_efficiency"),
            spark_avg("realization_rate").alias("realization_rate"),
        )

        # TABLES JOIN
        df_displacement = (
            df_asylum_requests.join(
                df_asylum_decisions, on=["year", "country_iso"], how="outer"
            )
            .join(df_idp_displacement, on=["year", "country_iso"], how="outer")
            .join(df_idp_returnees, on=["year", "country_iso"], how="outer")
            .join(df_naturalization, on=["year", "country_iso"], how="outer")
            .join(df_resettlement, on=["year", "country_iso"], how="outer")
        )

        df_displacement = df_displacement.withColumn(
            "id", concat_ws("_", col("year").cast("string"), col("country_iso"))
        )

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_displacement.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_displacement, kpi_name, configuration)


if __name__ == "__main__":
    DashboardSummaryJob().execute()

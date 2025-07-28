from argparse import Namespace

from pyspark.sql.functions import col, coalesce, when, lit, round, concat_ws

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "resettlements_summary_kpi"

        # DATAFRAMES LOAD

        df_resettlementsubmissionrequests = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="resettlementsubmissionrequests"
        )
        df_resettlementsubmissions = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="resettlementsubmissions"
        )
        df_resettlementdepartures = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="resettlementdepartures"
        )
        df_resettlementneeds = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="resettlementneeds"
        )

        # TABLES JOIN

        column_joins = [
            "year",
            "country_of_asylum_iso",
            "country_of_origin_iso",
            "country_of_resettlement_iso",
        ]
        df_requests_departures = (
            df_resettlementsubmissionrequests.alias("req")
            .join(df_resettlementdepartures.alias("dep"), on=column_joins, how="outer")
            .select(
                *column_joins,
                coalesce(
                    col("req.country_of_asylum"), col("dep.country_of_asylum")
                ).alias("country_of_asylum"),
                coalesce(
                    col("req.country_of_origin"), col("dep.country_of_origin")
                ).alias("country_of_origin"),
                coalesce(
                    col("req.country_of_resettlement"),
                    col("dep.country_of_resettlement"),
                ).alias("country_of_resettlement"),
                coalesce(col("req.cases"), lit(0)).alias("cases"),
                coalesce(col("req.persons"), lit(0)).alias("persons"),
                coalesce(col("dep.total"), lit(0)).alias("departures_total"),
            )
        )
        column_joins = ["year", "country_of_origin_iso", "country_of_asylum_iso"]
        df_requests_departures_needs = (
            df_requests_departures.alias("rd")
            .join(df_resettlementneeds.alias("need"), on=column_joins, how="outer")
            .select(
                *column_joins,
                coalesce(
                    col("rd.country_of_asylum"), col("need.country_of_asylum")
                ).alias("country_of_asylum"),
                coalesce(
                    col("rd.country_of_origin"), col("need.country_of_origin")
                ).alias("country_of_origin"),
                col("rd.country_of_resettlement_iso"),
                col("rd.country_of_resettlement"),
                col("rd.cases"),
                col("rd.persons"),
                col("rd.departures_total"),
                coalesce(col("need.total"), lit(0)).alias("total_needs"),
            )
        )

        column_joins = ["year", "country_of_origin_iso", "country_of_resettlement_iso"]
        df_requests_departures_needs_submissions = (
            df_requests_departures_needs.alias("rdn")
            .join(df_resettlementsubmissions.alias("sub"), on=column_joins, how="outer")
            .select(
                "year",
                "country_of_origin_iso",
                coalesce(
                    col("rdn.country_of_origin"), col("sub.country_of_origin")
                ).alias("country_of_origin"),
                col("rdn.country_of_asylum_iso"),
                col("rdn.country_of_asylum"),
                "country_of_resettlement_iso",
                coalesce(
                    col("rdn.country_of_resettlement"),
                    col("sub.country_of_resettlement"),
                ).alias("country_of_resettlement"),
                col("rdn.cases"),
                col("rdn.persons"),
                col("rdn.departures_total"),
                col("rdn.total_needs"),
                col("sub.total").alias("submissions_total"),
            )
        )

        df_requests_departures_needs_submissions = (
            df_requests_departures_needs_submissions.withColumn(
                "resettlement_gap", col("total_needs") - col("departures_total")
            )
            .withColumn(
                "coverage_rate",
                round(
                    when(
                        col("total_needs") > 0,
                        col("departures_total") / col("total_needs"),
                    ),
                    3,
                ),
            )
            .withColumn(
                "request_vs_needs_ratio",
                round(
                    when(col("total_needs") > 0, col("persons") / col("total_needs")), 3
                ),
            )
            .withColumn(
                "submissions_efficiency",
                round(
                    when(
                        col("submissions_total") > 0,
                        col("persons") / col("submissions_total"),
                    ),
                    3,
                ),
            )
            .withColumn(
                "realization_rate",
                round(
                    when(
                        col("submissions_total") > 0,
                        col("departures_total") / col("submissions_total"),
                    ),
                    3,
                ),
            )
        )

        df_requests_departures_needs_submissions = (
            df_requests_departures_needs_submissions.withColumn(
                "id",
                concat_ws(
                    "_",
                    col("year").cast("string"),
                    col("country_of_asylum_iso"),
                    col("country_of_origin_iso"),
                    col("country_of_resettlement_iso"),
                ),
            )
        )

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_requests_departures_needs_submissions.write.parquet(
            output_directory, mode="overwrite"
        )

        self._save_in_database(
            df_requests_departures_needs_submissions, kpi_name, configuration
        )


if __name__ == "__main__":
    (ResettlementSummaryJob().execute(),)

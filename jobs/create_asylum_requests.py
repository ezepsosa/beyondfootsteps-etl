from argparse import Namespace

from pyspark.sql.functions import col, try_divide, lit

from sparklibs.job import GoldJob, Configuration


class AsylumRequestsJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "asylum_requests_kpi"

        # DATAFRAMES LOAD

        df_asylumapplications = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="asylumapplications"
        )
        df_worldpopulation = self._get_last_version_from_silver(
            configuration, origin="WorldBank", entity="worlddevelopmentindicators"
        )

        # TABLES JOIN
        
        df_asylum_requests = (
            df_asylumapplications.alias("applications")
            .join(
                df_worldpopulation.alias("population"),
                on=[
                    col("applications.year") == col("population.year"),
                    col("applications.country_of_asylum_iso")
                    == col("population.country_code"),
                ],
                how="left",
            )
            .select(
                col("applications.year"),
                col("applications.country_of_origin"),
                col("applications.country_of_origin_iso"),
                col("applications.country_of_asylum"),
                col("applications.country_of_asylum_iso"),
                col("applications.applied"),
                col("population.population"),
            )
            .withColumn(
                "applied_per_100k",
                try_divide(col("applications.applied"), col("population.population"))
                * lit(100000),
            )
        )

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_asylum_requests.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_asylum_requests, kpi_name, configuration)


if __name__ == "__main__":
    (AsylumRequestsJob().execute(),)

from argparse import Namespace

from pyspark.sql.functions import col, sum, coalesce

from sparklibs.job import GoldJob, Configuration


class IdpReturneesJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "idp_returnees_kpi"

        # DATAFRAMES LOAD

        df_idpreturnees = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="idpreturnees"
        )
        df_refugeereturnees = (
            self._get_last_version_from_silver(
                configuration, origin="UNHCR", entity="refugeereturnees"
            )
            .groupBy("year", "country_of_origin_iso", "country_of_origin", "by_date")
            .agg(sum("total").alias("total"))
        )
        # TABLES JOIN
        column_joins = ["year", "country_of_origin_iso"]
        df_idp = (
            df_idpreturnees.alias("idpreturnees")
            .join(
                df_refugeereturnees.alias("refugeereturnees"),
                on=column_joins,
                how="outer",
            )
            .select(
                col("idpreturnees.id_idpreturnees"),
                *column_joins,
                coalesce(
                    col("idpreturnees.country_of_origin"),
                    col("refugeereturnees.country_of_origin"),
                ).alias("country_of_origin"),
                col("idpreturnees.total").alias("idp_returnees"),
                col("refugeereturnees.total").alias("refugees_returnees"),
                coalesce(col("idpreturnees.by_date")),
                col("refugeereturnees.by_date").alias("by_date"),
            )
        ).withColumnRenamed("id_idpreturnees", "id")

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_idp.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_idp, kpi_name, configuration)


if __name__ == "__main__":
    (IdpReturneesJob().execute(),)

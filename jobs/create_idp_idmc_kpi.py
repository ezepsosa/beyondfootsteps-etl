from argparse import Namespace

from pyspark.sql.functions import col, try_divide, lit

from sparklibs.job import GoldJob, Configuration


class IdpIdmcJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "idp_displacement_kpi"

        # DATAFRAMES LOAD

        df_idpreturnees = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="idpidmc"
        )
        df_population = self._get_last_version_from_silver(
            configuration, origin="WorldBank", entity="worlddevelopmentindicators"
        )
        # TABLES JOIN

        df_displacement = (
            df_idpreturnees.alias("idpreturnees")
            .join(
                df_population.alias("population"),
                on=[
                    (col("population.year") == col("idpreturnees.year"))
                    & (
                        col("idpreturnees.country_of_origin_iso")
                        == col("population.country_code")
                    )
                ],
                how="left",
            )
            .withColumn(
                "displacement_rate_per_100k",
                try_divide(col("idpreturnees.total"), col("population.population"))
                * lit(100000),
            )
            .select(
                col("idpreturnees.id_idpidmc"),
                col("idpreturnees.year"),
                col("idpreturnees.country_of_origin"),
                col("idpreturnees.country_of_origin_iso"),
                col("idpreturnees.total"),
                col("displacement_rate_per_100k"),
            )
        ).withColumnRenamed("id_idpidmc", "id") 

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_displacement.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_displacement, kpi_name, configuration)


if __name__ == "__main__":
    (IdpIdmcJob().execute(),)

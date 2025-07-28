from argparse import Namespace

from pyspark.sql.window import Window

from pyspark.sql.functions import lag, col, try_divide

from sparklibs.job import GoldJob, Configuration


class RefugeeNaturalizationJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):
        kpi_name = "refugee_naturalization_kpi"

        # DATAFRAMES LOAD

        df_naturalization = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="refugeenaturalization"
        ).select(
            "id_refugeenaturalization",
            "year",
            "country_of_origin",
            "country_of_origin_iso",
            "country_of_asylum",
            "country_of_asylum_iso",
            "total",
            "intake_date",
        )

        window_spec = Window.partitionBy(
            "country_of_asylum_iso", "country_of_origin_iso"
        ).orderBy("year")

        df_naturalization = (
            df_naturalization.withColumn(
                "previous_total", lag("total").over(window_spec)
            )
            .withColumn(
                "naturalization_change",
                try_divide(
                    (col("total") - col("previous_total")), col("previous_total")
                ),
            )
            .drop(col("previous_total"))
        ).withColumnRenamed("id_refugeenaturalization", "id")

        output_directory = f"{configuration.__getattribute__('output_dir')}/{kpi_name}"

        df_naturalization.write.parquet(output_directory, mode="overwrite")

        self._save_in_database(df_naturalization, kpi_name, configuration)


if __name__ == "__main__":
    (RefugeeNaturalizationJob().execute(),)

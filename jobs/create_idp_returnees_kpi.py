from argparse import Namespace

from pyspark.sql.functions import col, sum

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):

    def run(self, configuration: Configuration, args: Namespace):

        # DATAFRAMES LOAD

        df_idpreturnees = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="idpreturnees")
        df_refugeereturnees = self._get_last_version_from_silver(configuration,
                                                        origin="UNHCR",
                                                        entity="refugeereturnees").groupBy("year", "country_of_origin_iso").agg(sum("total").alias("total"))
        # TABLES JOIN

        df_idp = df_idpreturnees.alias("idpreturnees").join(
            df_refugeereturnees.alias("refugeereturnees"),
            on=[(col("refugeereturnees.year") == col("idpreturnees.year")) & (col("refugeereturnees.country_of_origin_iso") == col("idpreturnees.country_of_origin_iso"))],
            how='left').select(col('idpreturnees.id_idpreturnees'),
                                col('idpreturnees.year'),
                                col('idpreturnees.country_of_origin'),
                                col('idpreturnees.total').alias('idp_returnees'),
                                col('refugeereturnees.total').alias('refugees_returnees'),
                                col("idpreturnees.by_date"))

        self._save_in_database(df_idp, "returnees_kpi",
                               configuration)


if __name__ == '__main__':
    ResettlementSummaryJob().execute(),

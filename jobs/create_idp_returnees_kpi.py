from argparse import Namespace

from pyspark.sql.functions import col, when, round

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):

    def run(self, configuration: Configuration, args: Namespace):

        # DATAFRAMES LOAD

        df_idpreturnees = self._get_last_version_from_silver(
            configuration, origin="UNHCR", entity="idpreturnees")
        df_idpidmc = self._get_last_version_from_silver(configuration,
                                                        origin="UNHCR",
                                                        entity="idpidmc")

        # TABLES JOIN

        df_idp = df_idpreturnees.alias("returnees").join(
            df_idpidmc.alias("idmc"),
            on=[col("idmc.id_idpidmc") == col("returnees.id_idpreturnees")],
            how='inner').select(col('returnees.id_idpreturnees'),
                                col('returnees.year'),
                                col('returnees.country_of_origin'),
                                col('returnees.total').alias('idp_returnees'),
                                col('idmc.total').alias('idp_idmc'),
                                col("idmc.by_date"))

        self._save_in_database(df_idp, "returnees_kpi",
                               configuration)


if __name__ == '__main__':
    ResettlementSummaryJob().execute(),

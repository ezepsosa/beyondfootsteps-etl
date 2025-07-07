from argparse import Namespace

from pyspark.sql.functions import col, when, round

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):

        # DATAFRAMES LOAD

        df_asylumapplications = self._get_last_version_from_silver(configuration, origin="UNHCR",
                                                                               entity="asylumapplications")
        df_worldpopulation = self._get_last_version_from_silver(configuration, origin="WorldBank",
                                                                        entity="worlddevelopmentindicators")
        
        # TABLES JOIN

        df_asylum_requests = df_asylumapplications.alias("applications").join(
            df_worldpopulation.alias("population"), on=[col("applications.year") == col("population.year"), col("applications.country_of_asylum_iso") == col("population.country_code")],
            how='inner').select(col('applications.year'), col('applications.country_of_asylum_iso'), col('applications.applied'), col('population.population')).withColumn("applied_per_100k", (col('applications.applied') / col('population.population') * 100000))
        
        self._save_in_database(df_asylum_requests, "asylum_requests_kpi", configuration)

        
if __name__ == '__main__':
    ResettlementSummaryJob().execute(),

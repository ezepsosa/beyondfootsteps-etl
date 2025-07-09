from argparse import Namespace

from pyspark.sql.functions import col, when, round

from sparklibs.job import GoldJob, Configuration


class ResettlementSummaryJob(GoldJob):
    def run(self, configuration: Configuration, args: Namespace):

        # DATAFRAMES LOAD

        df_resettlementsubmissionrequests = self._get_last_version_from_silver(configuration, origin='UNHCR',
                                                                               entity='resettlementsubmissionrequests')
        df_resettlementsubmissions = self._get_last_version_from_silver(configuration, origin='UNHCR',
                                                                        entity='resettlementsubmissions')
        df_resettlementdepartues = self._get_last_version_from_silver(configuration, origin='UNHCR',
                                                                      entity='resettlementdepartures')
        df_resettlementneeds = self._get_last_version_from_silver(configuration, origin='UNHCR',
                                                                  entity='resettlementneeds')

        # TABLES JOIN

        df_requests_departures = df_resettlementsubmissionrequests.alias('requests').join(
            df_resettlementdepartues.alias('departures'), on=[col('requests.year') == col('departures.year'),
                                                              col('requests.country_of_asylum_iso') == col(
                                                                  'departures.country_of_asylum_iso'),
                                                              col('requests.country_of_origin_iso') == col(
                                                                  'departures.country_of_origin_iso'),
                                                              col('requests.country_of_resettlement_iso') == col(
                                                                  'departures.country_of_resettlement_iso'), ],
            how='left').select(col('requests.year'), col('requests.country_of_asylum'),
                               col('requests.country_of_origin'), col('requests.country_of_resettlement'),
                               col('requests.country_of_asylum_iso'), col('requests.country_of_origin_iso'),
                               col('requests.country_of_resettlement_iso'), col('requests.cases'),
                               col('requests.persons'), col('departures.total').alias('departures_total'))

        df_requests_departures_needs = df_requests_departures.alias('requests_departures').join(
            df_resettlementneeds.alias('needs'), on=[col('requests_departures.year') == col('needs.year'),
                                                     col('requests_departures.country_of_asylum_iso') == col(
                                                         'needs.country_of_asylum_iso'),
                                                     col('requests_departures.country_of_origin_iso') == col(
                                                         'needs.country_of_origin_iso'), ], how='left').select(
            col('requests_departures.year'), col('requests_departures.country_of_asylum'),
            col('requests_departures.country_of_origin'), col('requests_departures.country_of_resettlement'),
            col('requests_departures.country_of_asylum_iso'), col('requests_departures.country_of_origin_iso'),
            col('requests_departures.country_of_resettlement_iso'), col('requests_departures.cases'),
            col('requests_departures.persons'), col('requests_departures.departures_total'),
            col('needs.total').alias('total_needs'))

        df_requests_departures_needs_submissions = df_requests_departures_needs.alias('rdn').join(
            df_resettlementsubmissions.alias('sub'), on=[col('rdn.year') == col('sub.year'),
                                                         col('rdn.country_of_origin_iso') == col(
                                                             'sub.country_of_origin_iso'),
                                                         col('rdn.country_of_resettlement_iso') == col(
                                                             'sub.country_of_resettlement_iso'), ], how='left').select(
            col('rdn.year'), col('rdn.country_of_asylum'), col('rdn.country_of_origin'),
            col('rdn.country_of_resettlement'), col('rdn.country_of_asylum_iso'), col('rdn.country_of_origin_iso'),
            col('rdn.country_of_resettlement_iso'), col('rdn.cases'), col('rdn.persons'), col('rdn.departures_total'),
            col('rdn.total_needs'), col('sub.total').alias('submissions_total'))

        df_clean = df_requests_departures_needs_submissions.fillna(
            {'departures_total': 0, 'submissions_total': 0, 'total_needs': 0})

        df_clean = df_clean.withColumn('resettlement_gap', col('total_needs') - col('departures_total')).withColumn(
            'coverage_rate',
            round(when(col('total_needs') > 0, col('departures_total') / col('total_needs')), 3)).withColumn(
            'request_vs_needs_ratio',
            round(when(col('total_needs') > 0, col('persons') / col('total_needs')), 3)).withColumn(
            'submissions_efficiency',
            round(when(col('submissions_total') > 0, col('persons') / col('submissions_total')), 3)).withColumn(
            'realization_rate',
            round(when(col('submissions_total') > 0, col('departures_total') / col('submissions_total')), 3))
        self._save_in_database(df_requests_departures_needs_submissions, 'resettlements_summary_kpi', configuration)

if __name__ == '__main__':
    ResettlementSummaryJob().execute(),

from create_resettlement_summary_job import ResettlementSummaryJob
from create_refugee_naturalization import RefugeeNaturalizationJob
from create_asylum_decisions import AsylumDecisionsJobs
from create_asylum_requests import AsylumRequestsJob
from create_idp_idmc_kpi import IdpIdmcJob
from create_idp_returnees_kpi import IdpReturneesJob


if __name__ == "__main__":
    ResettlementSummaryJob().execute()
    RefugeeNaturalizationJob().execute()
    AsylumDecisionsJobs().execute()
    AsylumRequestsJob().execute()
    IdpIdmcJob().execute()
    IdpReturneesJob().execute()
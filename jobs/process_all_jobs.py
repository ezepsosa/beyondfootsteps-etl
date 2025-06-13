from process_unhcr_asylumapplications import ProcessUnhcrAsylumApplications
from process_unhcr_asylumdecisions import ProcessUnhcrAsylumDecisions
from process_unhcr_idpidmc import ProcessUnhcrIdpidmc
from process_unhcr_idpreturnees import ProcessUnhcrIdpreturnees
from process_unhcr_nowcasting import ProcessUnhcrNowcasting
from process_unhcr_palestinerefugees import ProcessUnhcrPalestineRefugees
from process_unhcr_population import ProcessUnhcrPopulation
from process_unhcr_refugeenaturalization import ProcessUnhcrRefugeeNaturalization
from process_unhcr_refugeereturnees import ProcessUnhcrRefugeereturnees
from process_unhcr_resettlementdepartures import ProcessUnhcrResettlementDepartures
from process_unhcr_resettlementneeds import ProcessUnhcrResettlementNeeds
from process_unhcr_resettlementsubmissions import  ProcessUnhcrResettlementSubmissions
from process_unhcr_resettlementsubmissionsrequests import ProcessUnhcrResettlementSubmissionsRequests

if __name__ == "__main__":
    ProcessUnhcrAsylumApplications().execute()
    ProcessUnhcrAsylumDecisions().execute()
    ProcessUnhcrIdpidmc().execute()
    ProcessUnhcrIdpreturnees().execute()
    ProcessUnhcrNowcasting().execute()
    ProcessUnhcrPalestineRefugees().execute()
    ProcessUnhcrPopulation().execute()
    ProcessUnhcrRefugeeNaturalization().execute()
    ProcessUnhcrRefugeereturnees().execute()
    ProcessUnhcrResettlementDepartures().execute()
    ProcessUnhcrResettlementNeeds().execute()
    ProcessUnhcrResettlementSubmissions().execute()
    ProcessUnhcrResettlementSubmissionsRequests().execute()

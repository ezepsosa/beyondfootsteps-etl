import logging, sys
from pathlib import Path
from urllib.parse import urlencode
import requests
sys.path.append(str(Path(__file__).resolve().parents[2]))
from libs.utils import load_config
import zipfile
import io
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class fetch_data():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Obtaining json config")
        self.config = load_config(r"C:\Users\EzequielPerez\Documents\Personal\Proyectos\MigrantScope\beyondfootsteps-etl\data\utils\config.json")
    
    def get_dataset_group(self, dataset_name):
        solutions_datasets = {
            "refugee_returnees", "idp_returnees", "refugee_naturalization",
            "resettlement_submissions", "resettlement_departures",
            "resettlement_arrivals", "resettlement_needs"
        }
        return "solutions" if dataset_name in solutions_datasets else "displacement"


    def build_unhcr_url(self, dataset_name):
        self.logger.info(f"Building URL for {dataset_name}")
        base_url = self.config['base_url']
        year_from = self.config['year_range']['from']
        year_to = self.config['year_range']['to']
        
        params = {
            "data-finder": "on",
            "data_finder[dataGroup]": self.get_dataset_group(dataset_name),
            "data_finder[dataset]": dataset_name,
            "data_finder[displayType]": "totals",
            "data_finder[year__filterType]": "range",
            "data_finder[year__rangeFrom]": year_from,
            "data_finder[year__rangeTo]": year_to,
            "data_finder[coo][displayType]": "displayAll",
            "data_finder[coa][displayType]": "displayAll"
        }
        
        population_types = self.config["datasets"].get(dataset_name, {}).get("population_types")
        if population_types:
            params["data_finder[populationType]"] = population_types
        return f"{base_url}?{urlencode(params, doseq=True)}"
    
    def download_unhcr_url(self, url, output_dir, dataset, date):
        self.logger.info(f"Starting download")
        headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/zip,application/octet-stream;q=0.9,*/*;q=0.8",
        "Referer": "https://www.unhcr.org/refugee-statistics/download"
        }

        response = requests.get(url, headers=headers)
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
            for file in zip.namelist():
                if file.endswith(".csv") and "footnotes" not in file.lower():
                    output_dir = f"{output_dir}/{date}"
                    filename = f"{dataset}_0.csv"
                    
                    os.makedirs(output_dir, exist_ok=True)
                    output_path = os.path.join(output_dir, filename)
                    with zip.open(file) as csv_file, open(output_path, "wb") as out_file:
                        out_file.write(csv_file.read())
                        self.logger.info("Dataset saved")
                    
    def run(self):
        datasets = self.config["datasets"]
        output_dir = self.config["output_dir"]
        self.logger.info("Downloading UNHCR data")
        date = datetime.now().strftime("%d%m%Y%H")
        for dataset in datasets:
            url = self.build_unhcr_url(dataset)
            try:
                if url:
                    self.download_unhcr_url(url, output_dir, dataset, date)
            except:
                self.logger.warning(f"Error downloading from {url}")
                
            
if __name__ == '__main__':
    fetch_data().run()
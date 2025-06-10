import logging, sys
from pathlib import Path
from urllib.parse import urlencode
import requests
sys.path.append(str(Path(__file__).resolve().parents[2]))
from libs.utils import load_config
import zipfile
import io

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class fetch_data():
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.info("Obtaining json config")
        self.config = load_config(Path(__file__).resolve().parent / "config.json")
        

    
    def build_unhcr_url(self, dataset_name):
        self.logger.info(f"Building URL for {dataset_name}")
        base_url = self.config['base_url']
        params = self.config['datasets'][dataset_name]['params']
        return f"{base_url}?{urlencode(params, doseq=True)}"
    
    def download_unhcr_url(self, url, output_dir, dataset):
        self.logger.info(f"Starting download")
        headers = self.config['headers']

        response = requests.get(url, headers=headers)
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
            for file in zip.namelist():
                if file.endswith(".csv") and "footnotes" not in file.lower():
                    base_dir = Path(__file__).resolve().parents[2] 
                    full_output_dir = base_dir / output_dir / "raw"  
                    print(f'saving in {full_output_dir}')
                    full_output_dir.mkdir(parents = True, exist_ok=True)
                    
                    filename = f"UNHCR_{dataset.replace("_","")}_0.csv"
                    output_path = full_output_dir / filename
                    
                    with zip.open(file) as csv_file, open(output_path, "wb") as out_file:
                        out_file.write(csv_file.read())
                        self.logger.info("Dataset saved")
                    
    def run(self):
        datasets = self.config["datasets"]
        output_dir = self.config["output_dir"]
        self.logger.info("Downloading UNHCR data")
        for dataset in datasets:
            url = self.build_unhcr_url(dataset)
            try:
                if url:
                    self.download_unhcr_url(url, output_dir, dataset)
            except Exception as e:
                self.logger.warning(f"Error downloading from {url}\n Error caused: {e}" )
                
            
if __name__ == '__main__':
    fetch_data().run()
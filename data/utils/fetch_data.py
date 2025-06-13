from argparse import Namespace
from os import mkdir
from pathlib import Path
from urllib.parse import urlencode
import zipfile
import io
import requests

from sparklibs.job import BaseJob, Configuration


class FetchData(BaseJob):

    def _load_config(self, configuration_path: str = None) -> Configuration:
        if configuration_path is None:
            return Configuration()
        else:
            return Configuration(configuration_path, self.__class__.__name__)

    def build_unhcr_url(self, dataset_name, base_url, params):
        self.logger.info(f"Building URL for {dataset_name}")
        return f"{base_url}?{urlencode(params, doseq=True)}"
    
    def download_unhcr_url(self, url, output_dir, dataset, request_headers):
        self.logger.info(f"Starting download")
        response = requests.get(url, headers=request_headers)

        with zipfile.ZipFile(io.BytesIO(response.content)) as zip:
            for file in zip.namelist():
                if file.endswith(".csv") and "footnotes" not in file.lower():
                    print(f'saving in {output_dir}')
                    if not Path(output_dir).exists():
                        Path(output_dir).mkdir(parents=True, exist_ok=True)
                    filename = f"UNHCR_{dataset.replace("_","")}_0.csv"
                    output_path = f'{output_dir}/{filename}'
                    
                    with zip.open(file) as csv_file, open(output_path, "wb") as out_file:
                        out_file.write(csv_file.read())
                        self.logger.info("Dataset saved")
                    
    def run(self, configuration: Configuration, args: Namespace):
        self._load_config(args.configuration)
        datasets = configuration.__getattribute__('datasets')
        output_dir = configuration.__getattribute__('output_dir')
        request_headers = configuration.__getattribute__('headers')
        self.logger.info("Downloading UNHCR data")
        for dataset in datasets:
            base_url = configuration.__getattribute__('base_url')
            params = configuration.__getattribute__('datasets')[dataset]['params']
            url = self.build_unhcr_url(dataset, base_url, params)
            try:
                if url:
                    self.download_unhcr_url(url, output_dir, dataset, request_headers)
            except Exception as e:
                self.logger.warning(f"Error downloading from {url}\n Error caused: {e}" )
                
            
if __name__ == '__main__':
    FetchData().execute()
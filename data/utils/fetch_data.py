from argparse import Namespace
from os import mkdir
from pathlib import Path
from urllib.parse import urlencode
import zipfile
import io
import requests
from io import BytesIO
import pandas as pd

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
    

    def download_unhcr_url(self, url, source, dataset, output_dir, string_not_included_in_file_name, request_headers):

        def build_file_name():
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            filename = f"{source}_{dataset.replace('_', '')}.csv"
            return Path(output_dir) / filename

        self.logger.info("Starting download")
        response = requests.get(url, headers=request_headers)

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            namelist = z.namelist()

            if any(f.endswith(".csv") for f in namelist) and '[Content_Types].xml' not in namelist:
                for file in namelist:
                    if file.endswith(".csv") and string_not_included_in_file_name.lower() not in file.lower():
                        output_path = build_file_name()
                        with z.open(file) as csv_file, open(output_path, "wb") as out_file:
                            out_file.write(csv_file.read())
                        self.logger.info(f"Dataset saved to {output_path}")

            else:
                output_path = build_file_name()
                xlsx_io = BytesIO(response.content)
                sheets = pd.read_excel(xlsx_io, sheet_name=None)
                first_df = next(iter(sheets.values()))

                first_df.to_csv(output_path, index=False, encoding='utf-8')
                self.logger.info(f"Excel converted and saved to {output_path}")
                    
    def run(self, configuration: Configuration, args: Namespace):
        self._load_config(args.configuration)

        sources = configuration.__getattribute__('sources')
        properties = configuration.__getattribute__('properties')
        output_dir = properties['output_dir']
        request_headers = properties['headers']
        self.logger.info("Downloading UNHCR data")
        for source in sources:
            for dataset_name, dataset_data in sources[source]['datasets'].items():
                base_url = sources[source]['base_url']
                string_not_included_in_file_name = sources[source]['string_not_included_in_file_name']
                params = dataset_data['params']
                url = self.build_unhcr_url(dataset_data, base_url, params)
                try:
                    if url:
                        self.download_unhcr_url(url, source, dataset_name, output_dir, string_not_included_in_file_name, request_headers)
                except Exception as e:
                    self.logger.warning(f"Error downloading from {url}\n Error caused: {e}" )
                        
                
if __name__ == '__main__':
    FetchData().execute()
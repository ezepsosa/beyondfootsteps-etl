import os
from argparse import Namespace, ArgumentParser
from sparklibs.job import BaseJob, Configuration
from pyspark.sql.functions import lit
from datetime import datetime
from typing import Tuple
from data.utils.dataSchemas import *


class IntakeData(BaseJob):
    
    def _config_args(self, parser: ArgumentParser) -> Namespace:
        parser.add_argument('--file', type=str, required=False)
        return super()._config_args(parser)
    
    def run(self, configuration: Configuration, args: Namespace):
        self._load_config(args.configuration)
        input_dir = configuration.__getattribute__('input_dir')
        output_dir = configuration.__getattribute__('output_dir')
        if not args.file:
            for file in os.listdir(input_dir):
                self.process_bronze_layer(file, input_dir, output_dir)
        else:
            self.process_bronze_layer(args.file)
        
    def process_bronze_layer(self, file, input_dir, output_dir):
        (file_name, origin, entity) = self.extract_data_from_path(file)
        self.logger.info(f'File to process: {file_name}')
        self.logger.info(f'Origin: {origin}')
        self.logger.info(f'Entity: {entity}')
        
        input_path = f'{input_dir}/{file_name}'
        output_path = f'{output_dir}/{origin}/{entity}'
        defined_schema = eval(f'schema_{origin}_{entity}')
        
        df = self.spark.read.option('header', 'true') \
            .option('multiLine', 'true') \
            .option('quote', '"') \
            .option('escape', '"') \
            .schema(defined_schema) \
            .csv(input_path)

        intake_date = datetime.now().strftime("%Y%m%d%H")
        self.logger.info(f'Applying intake date: {intake_date}')

        df = df.withColumn('intakeDate', lit(intake_date))
        self.logger.info(f'Saving parquet of entity: {entity}')
        df.write.partitionBy('intakeDate').parquet(output_path, mode="append")
        self.logger.info(f'Parquet successfully saved in {output_path}')

    @staticmethod
    def extract_data_from_path(path: str) -> Tuple[str, str, str]:
        file_name = os.path.basename(path)
        file_name_parts = file_name.split("_")
        
        if len(file_name_parts) < 3:
            raise Exception(f'invalid file name: {file_name}')
        origin = file_name_parts[0]
        entity = file_name_parts[1]
        return file_name, origin, entity
    
if __name__ == '__main__':
    IntakeData().execute()
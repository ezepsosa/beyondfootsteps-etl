import logging
import argparse
from argparse import ArgumentParser, Namespace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_date
import json
import re

class Configuration:
    def __init__(self, config_file: str = None, job_name: str = None):
        self.logger = logging.getLogger(__name__)
        if config_file and job_name:
            properties = self.load_json(config_file)
            if job_name in properties:
                for prop in properties[job_name]:
                    setattr(self, prop, properties[job_name][prop])
            if 'db_configuration' in properties:
                for prop in properties['db_configuration']:
                    setattr(self, prop, properties['db_configuration'][prop])
    @staticmethod
    def load_json(config_file):
        with open(config_file, 'r') as file:
            return json.load(file)


class BaseJob:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._config_logger()
        self.spark = SparkSession.builder.appName("BeyondFootstepsETL").config("spark.jars", "./jars/postgresql-42.7.4.jar").getOrCreate()

    def _load_config(self, configuration_path: str = None) -> Configuration:
        if not configuration_path:
            return Configuration()

        return Configuration(configuration_path, self.__class__.__name__)
    
    def _config_args(self, parser: ArgumentParser) -> Namespace:
        parser.add_argument('--configuration', type=str, required=True)
        return parser.parse_args()
    
    def execute(self):
        parser = argparse.ArgumentParser(description=f'Job: {self.__class__.__name__}')
        args = self._config_args(parser)

        configuration = self._load_config(args.configuration)
        self.run(configuration, args)
        self.spark.stop()

        
    def run(self, configuration: Configuration, args: Namespace):
        raise Exception('Function not implemented')
        
    @staticmethod
    def _config_logger():
        logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    logger = logging.getLogger('sparklibs')
    logger.setLevel(logging.DEBUG)

class SilverProcessor(BaseJob):
    def __init__(self, origin: str, entity: str, id_column_name: str = "id"):
        super().__init__()
        self.origin = origin
        self.entity = entity
        self.id_column_name = id_column_name
        
    def _config_args(self, parser):
        parser.add_argument('--intake_date', type=str, required=True)
        return super()._config_args(parser)

    def _load_data_from_bronze_layer(self, input_dir:str, intake_date: str) -> DataFrame:
        df = self.spark.read.parquet(input_dir).where(col('intakeDate') == intake_date)
        self.logger.info(f'Loaded bronze layer with intake date {intake_date} which has {df.count()} rows')
        return df

    @staticmethod
    def process_column_names(column):
        col_name = column.strip()
        col_name = re.sub(r'[\s\.\-]+', '_', col_name)
        col_name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', col_name)
        col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
        col_name = re.sub(r'__+', '_', col_name)
        return col_name.lower()

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        return df

    def names_to_snake_case(self, df: DataFrame) -> DataFrame:
        column_names = [self.process_column_names(column) for column in df.columns]
        for old_name, new_name in zip(df.columns, column_names):
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def proccess_id_column(self, df: DataFrame) -> DataFrame:
        if 'id_' in str(df.columns):
            return df.drop(col('id'))
        else:
            return df.withColumnRenamed('id', self.id_column_name)

    def run(self, configuration: Configuration, args: Namespace):

        input_dir = f'{configuration.__getattribute__('input_dir')}/{self.origin}/{self.entity}'
        output_dir = f'{configuration.__getattribute__('output_dir')}/{self.origin}/{self.entity}'
        bronze_df = self._load_data_from_bronze_layer(input_dir=input_dir, intake_date=args.intake_date)
        bronze_df = self._preprocess_dataframe(bronze_df)
        bronze_df = bronze_df.withColumn('id', col(self.id_column_name))
        bronze_df = bronze_df.withColumn(
                'by_date',
                when(
                    col('intakeDate').cast('string').rlike(r'^\d{10}$'),
                    to_date(col('intakeDate').cast('string'), 'yyyyMMddHH')
                )
                .otherwise(col('intakeDate').cast('string').cast('date'))
            )

        bronze_df = self.names_to_snake_case(bronze_df)
        bronze_df = self.proccess_id_column(bronze_df)

        bronze_df.write.parquet(output_dir, mode='overwrite')
        self.logger.info(f'Dataframe {self.origin} successfully processed')


class GoldJob(BaseJob):
    def _get_last_version_from_silver(self, config: Configuration, origin: str, entity: str) -> DataFrame:
        input_dir = f'{config.__getattribute__('input_dir')}/{origin}/{entity}'
        df = self.spark.read.parquet(input_dir)
        self.logger.info(f'Loaded silver layer of {entity} entity which has {df.count()} rows')
        return df
    
    def _get_last_version_from_gold(self, config: Configuration, entity: str) -> DataFrame:
        output_dir = f'{config.__getattribute__('output_dir')}/{entity}'
        df = self.spark.read.parquet(output_dir)
        self.logger.info(f'Loaded gold layer of {entity} gold entity which has {df.count()} rows')
        return df

    def _save_in_database(self, df: DataFrame, table_name: str, config: Configuration):
        self.logger.info(f'Saving {table_name} into database')
        db_configuration = config.__getattribute__('db_credentials')
        db_url = config.__getattribute__('database_url')
        exists = self._check_table_exists(table_name, db_configuration, db_url)

        if exists:
            self.logger.info(f'Table {table_name} already exists, truncating and appending new data')
            self._truncate_table(db_url,table_name, db_configuration)
            df.write.jdbc(db_url, table_name, mode='append', properties=db_configuration)
        else:
            self.logger.info(f'Table {table_name} does not exist, creating it and inserting data')
            df.write.jdbc(db_url, table_name, mode='overwrite', properties=db_configuration)
        self.spark.stop()

    def _check_table_exists(self, table_name, db_properties: dict, db_url: str) -> bool:
        query = f"(SELECT to_regclass('{table_name}') AS exists_flag) AS subquery"
        check = self.spark.read.jdbc(db_url, query, properties=db_properties)
        return check.first()["exists_flag"] is not None

    def _truncate_table(self, jdbc_url: str, table: str, db_properties: dict) -> None:
        self.logger.info(f'Executing TRUNCATE on table: {table}')
        try:
            jvm = self.spark._jvm
            DriverManager = jvm.java.sql.DriverManager
            conn = DriverManager.getConnection(jdbc_url, db_properties['user'], db_properties['password'])
            stmt = conn.createStatement()
            stmt.executeUpdate(f'TRUNCATE TABLE {table} CASCADE')
            self.logger.info(f"Table {table} truncated successfully.")
            stmt.close()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error while truncating table {table}: {e}")
            raise
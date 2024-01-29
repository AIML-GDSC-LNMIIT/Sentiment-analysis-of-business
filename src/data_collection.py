from src.logger import logging
from src.exception import CustomException
import pandas as pd
from dataclasses import dataclass
import os
@dataclass
class DataCollectionConfig:
    raw_data_path:str = os.path.join("/config/workspace/artifacts", "raw.csv")

class DataCollection:     
    def __init__(self):
        self.collection_config = DataCollectionConfig()

    def initiate_data_collection(self):
        logging.info("Data Collection Method Starts")
        try:
            df = pd.read_csv(os.path.join('/config/workspace/notebooks/data','final_dataset.csv'))
            logging.info("Dataset read as pandas dataframe")

            os.makedirs(os.path.dirname(self.collection_config.raw_data_path), exist_ok=True)
            df.to_csv(self.collection_config.raw_data_path, index=False)
            logging.info("raw data is read and it is send to Artifacts folder")

            return(
                self.collection_config.raw_data_path
            )

        except Exception as e:
            logging.info("Exception Occured at Data Collection Stage")
            raise CustomException(e,sys)

if __name__ == "__main__":
    obj = DataCollection()
    raw_data_path = obj.initiate_data_ingestion() 
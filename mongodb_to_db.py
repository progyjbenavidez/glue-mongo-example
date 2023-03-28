
from sqlalchemy import create_engine
from typing import List, Dict
import pandas as pd 
import pymongo


class MongoService:
    """
    This module is use to a migrate a mongodb collection to a table on a relational db
    Make sure the table exist before running the db
    """
    #Mongo Creds
    MONGO_DB_HOST=""
    MONGO_DB_DATABASE=""
    MONGO_DB_USER=""
    MONGO_DB_PASSWORD=""
    MONGO_COLLECTION_NAME=""
    #db  creds
    DB_HOST=""
    DB_USER=""
    DB_PASSWORD=""
    DB_NAME=""
    DB_PORT=3306


    @classmethod
    def get_mongo_collection(cls):
        """ Return Mongo collectio  """
        conn = pymongo.MongoClient(f'mongodb://{cls.MONGO_DB_USER}:{cls.MONGO_DB_PASSWORD}@{cls.MONGO_DB_HOST}:27017/')
        db = conn[cls.MONGO_DB_DATABASE]
        collection = db[cls.MONGO_COLLECTION_NAME]
        return collection

    @classmethod
    def get_db_engine(cls):
        return create_engine(f'mysql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}')

    @staticmethod
    def entity_to_record(record) -> Dict:
        """ Convert entity to dict migrate only required field"""
        return {
                "key": "value"
            }
    @classmethod
    def gell_all_collection(cls) -> List[str]:
        return list(cls.db.list_collection_names()) 

    @classmethod
    def get_docs(cls) -> List:
        """ Get all docs on collections"""
        print("getting docs from collection......")
        collection  = cls.get_mongo_collection()
        cursor = collection.find({})
        #return [cls.entity_to_record(item) for item in cursor]
        return list(cursor)

    @classmethod
    def docs_to_csv(cls) -> bool:
        """ Export docs to CSV """
        mongo_docs = cls.get_docs()
        df = pd.DataFrame(mongo_docs)
        print("Saving docs......")
        output_file = f"{cls.MONGO_COLLECTION_NAME}_docs.csv"
        print(f"df size {df.size}")
        print(f"{output_file} created succefuly......")
        # export to csv to keep a copy of the records 
        df.to_csv(output_file, ",", index=False)
        return True

    @classmethod
    def csv_to_db(cls) -> bool:
        """Insert data from CSV to DB """
        df = pd.read_csv(f"{cls.MONGO_COLLECTION_NAME}_docs.csv")
        df.to_sql('users', con=cls.get_db_engine()) 
    

    @classmethod
    def init_process(cls):
        """Orchestrate Workflow"""
        # convert docs to csv 
        print("Converting docs to cvs ....")
        cls.docs_to_csv()
        print("Inserting record from cvs to db .....")
        cls.csv_to_db()  
        print("Records were saved succefully ....")

if  __name__ == "__main__":
    MongoService.init_process()

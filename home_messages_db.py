from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base  
from sqlalchemy.orm import sessionmaker                  
from sqlalchemy.exc import IntegrityError                
from sqlalchemy import and_
from sqlalchemy import or_
import pandas as pd
import numpy as np

"""
First, the structure of the database is defined in the classes below. 
The class HomeMessagesDB is responsible for communicating with the database. 
"""

Base = declarative_base()
class Electricity(Base):
    """
    Concept for the 'electricity' table of the database
    """
    __tablename__ = 'electricity'                                             
    epoch = Column(Integer, primary_key=True)  
    T1 = Column(Float)                         
    T2 = Column(Float)                        

class Gas(Base):
    """
    Concept for the 'gas' table of the database
    """
    __tablename__ = 'gas'  

    epoch = Column(Integer, primary_key=True)  
    usage = Column(Float)                      

class SmartThings(Base):
    """
    Concept for the 'smartthings' table of the database
    """
    __tablename__ = 'smartthings'  
    id = Column(String, primary_key=True)
    epoch = Column(Integer, nullable=False)                
    loc = Column(String)
    level = Column(String)
    name = Column(String)
    capability = Column(String)                
    attribute = Column(String)                 
    value = Column(String)                    
    unit = Column(String)                      

class HomeMessagesDB:

    """
    The class contains methods to initialize the database at the provided address, ingest the data, and query it. 
    """

    def __init__(self, db_url):
        self.engine = create_engine(db_url)                
        Base.metadata.create_all(self.engine)              
        self.Session = sessionmaker(bind=self.engine)      
        self.session = self.Session()                      

    def insert_p1e_data(self, input_df,table_name='electricity'):
        """
        The function inserts the data on the electricity consumption into the corresponding table of the database.
        First, it calls the compare_entires() function (described below) to prevent insertion of duplicate records.
        It then inserts the records in a time-efficient manner.  
        """
        try: 
            current_num_rows=self.count_rows(table_name)
            print(f"Current number of electricity readings: {current_num_rows} are currently in the database table '{table_name}'.")
            print('Number of candidate rows: ', input_df.shape[0])
            new_info_df=self.compare_entires(input_df,table_name)

            print('Completely new rows: ', new_info_df.shape[0])
            self.session.bulk_insert_mappings(Electricity, new_info_df.to_dict('records'))
            self.session.commit()      
            print(f"Successfully inserted {new_info_df.shape[0]} new electricity readings into database.")

            updated_num_rows=self.count_rows(table_name)
            print(f"Updated number of electricity readings: {updated_num_rows} are currently in the database table '{table_name}'.")
        
        except IntegrityError: # Helps to catch the cases with duplicates in keys, for instance
            print('Conflict!')
            self.session.rollback()  # Rollback to prevent that corrupted entries will be inserted

    def insert_p1g_data(self, input_df,table_name='gas'):
        """
        The function is equivalent to insert_p1e_data() above, and inserts the data on gas usage
        into the corresponding database table.
        """
        try:
            current_num_rows=self.count_rows(table_name)
            print(f"Current number of gas readings: {current_num_rows} are currently in the database table '{table_name}'.")
            print('Number of candidate rows: ', input_df.shape[0])
            new_info_df=self.compare_entires(input_df,table_name)

            print('Completely new rows: ', new_info_df.shape[0])
            self.session.bulk_insert_mappings(Gas, new_info_df.to_dict('records'))
            self.session.commit()     
            print(f"Successfully inserted {new_info_df.shape[0]} new gas readings into database.")

            updated_num_rows=self.count_rows(table_name)
            print(f"Updated number of gas readings: {updated_num_rows} are currently in the database table '{table_name}'.")
        except IntegrityError:
            print('Conflict!')
            self.session.rollback()    

    def insert_smartthings(self,input_df,table_name='smartthings'):
        """
        The function is equivalent to the two insertion functions above, and ingests 
        the data collected by the smart devivces into the corresponding database table. 
        """
        try:
            current_num_rows=self.count_rows(table_name)
            print(f"Current number of smartthings readings: {current_num_rows} are currently in the database table '{table_name}'.")
            print('Number of candidate rows: ', input_df.shape[0])
            new_info_df=self.compare_entires(input_df,table_name)
            print('Completely new rows: ', new_info_df.shape[0])
            self.session.bulk_insert_mappings(SmartThings, new_info_df.to_dict('records'))
            self.session.commit()     
            print(f"Successfully inserted {new_info_df.shape[0]} new smartthings readings into database.")
            updated_num_rows=self.count_rows(table_name)
            print(f"Updated number of smartthings readings: {updated_num_rows} are currently in the database table '{table_name}'.")

        except IntegrityError:
            print('Conflict!')
            self.session.rollback()  
        

    def count_rows(self, table_name):
        """
        The function enables to retrieve the current number of records in a database table  
        """
        if table_name == 'electricity':
          return self.session.query(Electricity).count()
        elif table_name == 'gas':
          return self.session.query(Gas).count()
        elif table_name == 'smartthings':
          return self.session.query(SmartThings).count()
        else:
          raise ValueError(f"Unknown table: {table_name}")  


    def compare_entires(self,input_df,table_name):
        """
        The function ensures that the database does not contain duplicate records.
        It retrieves the current records of the database table of interest, and compares their identifiers  
        (based on either the id or the epoch, depending on the table) to the identifiers of the data to be inserted 
        in the same table. Only the new records are ingested in the database table.
        """
        # Read in the current records
        if table_name == 'electricity':
            all_entries=self.session.query(Electricity)
        elif table_name == 'gas':
            all_entries=self.session.query(Gas)
        elif table_name == 'smartthings':
            all_entries=self.session.query(SmartThings)

        current_df=pd.read_sql(all_entries.statement,self.session.bind) 

        # For smartthings:
        if table_name=='smartthings':
            if current_df.shape[0]!=0: # If the table is not empty:
                # Get the unique records in both dataframes
                unique_current=current_df['id'].unique() #There should be no duplicates; line left as a precaution.
                unique_input=input_df['id'].unique()
                # Retrieve the unique identifiers of the current data, and the data to be ingested
                set_current = set(unique_current)
                set_input = set(unique_input)
                unique_to_add=list(set_input.difference(set_current))
                # Only choose the new rows to be inserted
                df_to_write=input_df[input_df.id.isin(unique_to_add)]
            else:
                df_to_write=input_df # If the table is empty, insert all records.

        # For gas and electricity:
        elif (table_name=='electricity' or table_name=='gas'): 
            # If there are records in the table:
            if current_df.shape[0]!=0:
                # Get the unique records in both dataframes
                unique_current=current_df['epoch'].unique()
                unique_input=input_df['epoch'].unique()
                unique_to_add=np.setdiff1d(unique_input,unique_current)
                # Filter the input dataframe to only include the unique epochs
                df_to_write=input_df[input_df.epoch.isin(unique_to_add)]
            else:
                df_to_write=input_df

        return df_to_write

    def query_smartthings(self):
        """
        Function to extract the temperature and humidity readings,
        produced by the sensor located in the garden, that are present in the database
        """
        out_query = self.session.query(SmartThings).filter(
            and_(
                SmartThings.name.like('Garden air (sensor)'),
                or_(SmartThings.attribute.like('temperature'), SmartThings.attribute.like('humidity'))
            )
        )
        out_df=pd.read_sql(out_query.statement,self.session.bind) 

        return out_df

    def query_gas(self):
        """
        Function to extract all gas readings, that are present in the database
        """
        out_query = self.session.query(Gas)
        out_df=pd.read_sql(out_query.statement,self.session.bind) 

        return out_df
    def query_electricity(self):
        """
        Function to extract all electricity readings, that are present in the database
        """
        out_query = self.session.query(Electricity)
        out_df=pd.read_sql(out_query.statement,self.session.bind) 

        return out_df




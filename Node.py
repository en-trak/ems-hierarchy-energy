import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from collections import defaultdict
from common import is_none_or_nan, is_none_or_nan_zero


class Node:

    def __init__(self, host="localhost", port=5432, database="node", user="node", password="node"):        
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)
    
    # serial="34EAE774A2AC"
    def devices(self, serial=""):               
        sql = ''' select id as node_device_id, serial as node_device_serial from devices '''
        if len(serial) > 0:
            sql = f''' select id as node_device_id, serial as node_device_serial from devices where serial = '{serial}' '''

        # Read the data from the table into the DataFrame
        # Specify data types for integer columns
        dtypes = {
            'node_device_id': str,
            'node_device_serial': str,
        }

        # Read the data into the DataFrame with specified data types
        dataDF = pd.read_sql_query(sql, self.engine, dtype=dtypes)

        return dataDF
    
import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

class Organization:

    def __init__(self, host="localhost", port=5432, database="organization", 
                 user="organization", password="organization"):        
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)

    def Tenant(self, code="cdnis"):      
        sql = f"select id, name, company_code from tenants where company_code = '{code}' "

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        return dataDF
    
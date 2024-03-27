import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from collections import defaultdict
from common import is_none_or_nan


class City:

    def __init__(self, host="localhost", port=5432, database="city", user="city", password="city"):        
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)

    def city(self, city_name="Wan Chai"):      
        sql = f"select id, city from cities where city = '{city_name}'"

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
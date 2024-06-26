import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import uuid
from enum import Enum
from common import zeroUUID

# Enum of meter defined in energy_meter.proto file
class DataType(Enum):
    NO_DATATYPE = 0
    CUMULATIVE = 1
    DELTA = 2
    MIXED = 3

class MeterType(Enum):
    NO_METERTYPE = 0
    DEFAULT = 1
    EGAUGE = 2
    BMS = 3
    ACREL = 4

class MeterStatus(Enum):
    NO_METERSTATUS = 0
    ACTIVE = 1
    INACTIVE = 2
    INSTALLED = 3
    IN_STOKC = 4

class DatapointStatus(Enum):
    NO_DATAPOINTTATUS = 0
    DISABLE = 1
    ENABLE = 2

class DataFlowMode(Enum):
    NO_DATAFLOWMODE = 0
    OLD_DATAFLOW = 1
    EGAUGE_METER_TO_MANGOPIE = 2
    HUB_METER_TO_MANGOPIE = 3

class BmsType(Enum):
    BMSNO = 0
    MODBUS = 1
    BACNET = 2



class Energy:
    DatapointStatus_name = {
        "NO_DATAPOINTTATUS": 0,
        "DISABLE": 1,
        "ENABLE": 2
    }  
   

    def __init__(self, host="localhost", port=5432, database="energy", user="energy", password="energy", logger = None):    
        self.logger = logger
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)


    def meter(self, columns = ["id", "ref_id", "name"]):        
        sql = f''' SELECT id, ref_id, status, tenant_id, failed_counts, report_last_sent, last_online_at, description, has_kva, dataflow_mode, site_id, data_type, meter_type, serial, remark
            FROM energy_meter '''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        return dataDF[columns]    
    
    
    def meterEgauge(self, columns = ["id", "xml_url"]):        
        sql = f''' SELECT meter_id as id, xml_url from energy_egaugemeter '''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        return dataDF[columns] 
    
    
    def meterBms(self, columns = ["id", "meter_type"]):        
        sql = f''' SELECT meter_id as id, type as meter_type, data_transmission_unit FROM energy_bmsmeter '''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        return dataDF[columns] 
    

    def dataPoint(self, columns = ["id", "ref_id", "name"]):        
        sql = f''' SELECT id, ref_id, meter_id, "name", status, device_id, object_id, object_type
            FROM energy_datapoint'''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        return dataDF[columns]
    
    
    def virtualDataPoint(self, columns = ["datapoint_id", "expression", "name"]):
        # Define the SQL query to select specific columns
        sql = f'''SELECT id, tenant_id, datapoint_id, name, expression FROM energy_virtual_datapoint'''
        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
       
        return dataDF[columns]
    

    def create_new_datapoint(self, ref_id, name, meter_id):  
       
        new_id = uuid.uuid4()

        status = self.DatapointStatus_name["ENABLE"]

        name = name.replace("'", "''")
        sql = f"""
            INSERT INTO energy_datapoint (id, ref_id, name, meter_id, status)
            VALUES ('{new_id}', '{ref_id}', '{name}', '{meter_id}', {status})
            RETURNING id;
        """

        with self.engine.connect() as connection:
            datapoint_id = connection.execute(sql).fetchone()[0]
            return datapoint_id
        
    
    def create_new_virtual_datapoint(self, tenant_id, datapoint_id, name, composition_expression):  
        """
        enum VirtualDatapointStatus {
        VDS_DEFAULT = 0;
        VDS_DISABLE = 1;
        VDS_ENABLE = 2;
        }
        """
        new_id = uuid.uuid4()
        name = name.replace("'", "''")
        sql = f"""
            INSERT INTO energy_virtual_datapoint
            (id, tenant_id, datapoint_id, "name", "expression", status)
            VALUES ('{new_id}', '{tenant_id}', '{datapoint_id}', '{name}', '{composition_expression}', 2)
            RETURNING id;
        """
        with self.engine.connect() as connection:
            datapoint_id = connection.execute(sql).fetchone()[0]

        return datapoint_id

    
    def update_virtual_datapoint(self, datapoint_id, composition_expression): 
        sql = f"""
            update energy_virtual_datapoint
            set expression = '{composition_expression}'
            where datapoint_id = '{datapoint_id}'
        """
        with self.engine.connect() as connection:
            connection.execute(sql)

    def getVirtualDataPoint(self, datapoint_id,  columns = ["id", "tenant_id", "datapoint_id", "name", "expression", "status", "is_solar"]):        
        sql = f'''SELECT id, tenant_id, datapoint_id, "name", "expression", status, is_solar
            FROM energy_virtual_datapoint  where datapoint_id = '{datapoint_id}'  '''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)

        if dataDF.empty:
            # DataFrame is empty (has no rows)
            return None

        return dataDF[columns]
    
    
    def create_new_meter(self, ref_id, name, 
                         dataflow_mode: DataFlowMode = DataFlowMode.NO_DATAFLOWMODE,                           
                         data_type: DataType = DataType.CUMULATIVE, 
                         meter_type: MeterType = MeterType.EGAUGE,
                         meter_status: MeterStatus = MeterStatus.ACTIVE):          

        if meter_type == MeterType.EGAUGE:
            pass
        elif meter_type == MeterType.BMS:
            pass
        else:
            # self.logger.info("no code for meter type: {}".format(meter_type))
            return None

        new_id = uuid.uuid4()
        site_id = zeroUUID()

        status = self.DatapointStatus_name["ENABLE"]
        name = name.replace("'", "''")
        sql = f"""
            INSERT INTO energy_meter (id, ref_id, name, status, dataflow_mode, site_id, data_type, meter_type, status)
            VALUES ('{new_id}', '{ref_id}', '{name}', {status}, {dataflow_mode}, {site_id}, {data_type}, {meter_type}, {meter_status})
            RETURNING id;
        """
        with self.engine.connect() as connection:
            meter_id = connection.fetchone()[0]
            return meter_id
        
    
    

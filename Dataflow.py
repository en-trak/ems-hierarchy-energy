import pandas as pd
import numpy as np
import pandas as pd
from EMS import EMS
from Energy import Energy
from Hierarchy import Hierarchy
from Organization import Organization
from City import City
from Node import Node
import xml.etree.ElementTree as ET
import re
from common import zeroUUID, is_none_or_nan, is_none_or_nan_zero, readOption, run_grpc, small_endian_uuid, STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC
import energy_virtual_datapoint_pb2 as vdpGrpcPb2 
import uuid
import gc
from pathlib import Path
import os


class DataFlow(object):
    simulation = False
    code = 'demohk'

    def __init__(self, code = None, components_binding=False, simulation = False, logger = None):     
        self.logger = logger
        self.simulation = simulation        
        self.code = code
        self.site_path = f"./output/{self.code}"
        if not Path(f"{self.site_path}").is_dir(): 
            os.makedirs(self.site_path)

        host=readOption("databases.hierarchy.host")
        port=readOption("databases.hierarchy.port")
        database=readOption("databases.hierarchy.database")
        user=readOption("databases.hierarchy.username")
        password=readOption("databases.hierarchy.password")

        self.hr = Hierarchy(host=host, port=port, user=user, password=password, 
                            database=database, simulation=simulation,
                            logger=logger)
        
        host=readOption("databases.organization.host")
        port=readOption("databases.organization.port")
        database=readOption("databases.organization.database")
        user=readOption("databases.organization.username")
        password=readOption("databases.organization.password")
        self.org = Organization(host=host, port=port, user=user, password=password, database=database)

        host=readOption("databases.ems.host")
        port=readOption("databases.ems.port")
        database=readOption("databases.ems.database")
        user=readOption("databases.ems.username")
        password=readOption("databases.ems.password")
        self.ems = EMS(host=host, port=port, user=user, password=password, database=database)

        host=readOption("databases.energy.host")
        port=readOption("databases.energy.port")
        database=readOption("databases.energy.database")
        user=readOption("databases.energy.username")
        password=readOption("databases.energy.password")
        self.energy = Energy(host=host, port=port, user=user, password=password, database=database)

        host=readOption("databases.city.host")
        port=readOption("databases.city.port")
        database=readOption("databases.city.database")
        user=readOption("databases.city.username")
        password=readOption("databases.city.password")
        self.city = City(host=host, port=port, user=user, password=password, database=database)

        host=readOption("databases.node.host")
        port=readOption("databases.node.port")
        database=readOption("databases.node.database")
        user=readOption("databases.node.username")
        password=readOption("databases.node.password")
        self.node = Node(host=host, port=port, user=user, password=password, database=database)
        

        self.code = code
        self.components_binding = components_binding
    
    
    def PreparingData(self):        
        company_df = self.ems.company(code=self.code)
        tenant_df = self.org.Tenant(code=self.code)
        joined_tenant = pd.merge(company_df, tenant_df, how="left", left_on="code", right_on="company_code")        
        tenant_df = joined_tenant[['id_x', 'id_y', 'name_x', 'code']]       
        
        new_names = {'id_x': 'company_id', 'id_y': 'tenant_id', 'name_x': 'company', "code": 'company_code'}
        tenant_df = tenant_df.rename(columns=new_names)
        tenant_df.to_csv(f"{self.site_path}/tenant_df.csv", index=False)
        tenant_df = pd.read_csv(f"{self.site_path}/tenant_df.csv", index_col=False)

        # self.logger.info("---------------Tenant-----------------")
        # self.logger.info(tenant_df[:3])

        sys_df = self.ems.systems(code=self.code)
        # print("----------------------------------------------------------------")
        # 因为某些列包含'nan'/ NaN / 5123.0导致无法做merge，暂时可以用to_csv,然后read_csv可以避免这个问题        
        sys_df.to_csv(f"{self.site_path}/sys_df.csv", index=False)
        sys_df = pd.read_csv(f"{self.site_path}/sys_df.csv", index_col=False)        
        sys_df['source_key'] = sys_df['source_key'].replace(np.nan, '')
        sys_df['meter_id'] = sys_df['meter_id'].replace(np.nan, '-1')
        sys_df['meter_id'] = sys_df['meter_id'].replace(np.nan, '-1')
        sys_df['meter_id'] = sys_df['meter_id'].astype(int)
        # print(sys_df[:2])
        if sys_df.shape[0] == 0:
            self.logger.error(f"---- sys_df is empty, so can't do migrate for the systems of {self.code} ----")   
            return None

        meters = self.energy.meter(columns = ["meter_id", "meter_ref_id"])
        meters.to_csv(f"{self.site_path}/meters.csv", index=False)
        meters = pd.read_csv(f"{self.site_path}/meters.csv", index_col=False)

        dp = self.energy.dataPoint(columns = ["dp_id", "ref_id", "source_key", "meter_id"])
        dp.to_csv(f"{self.site_path}/dp.csv", index=False)
        dp = pd.read_csv(f"{self.site_path}/dp.csv", index_col=False)

        meter_dp_df = pd.merge(meters, dp, how="left", left_on="meter_id", right_on="meter_id")
        meter_dp_df = meter_dp_df[['meter_ref_id', 'source_key', 'dp_id', 'ref_id']]
        # Drop rows with any NaN values
        meter_dp_df.dropna(inplace=True)
        meter_dp_df = meter_dp_df[(meter_dp_df['source_key']!='nan') &  (~pd.isnull(meter_dp_df['source_key'])) & (~pd.isnull(meter_dp_df['dp_id']))]   
        
        # print("----------------------------------------------------------------")
        # print(meter_dp_df.isnull().sum())
        # print(meter_dp_df.shape)
        # print("-----------")        
        meter_dp_df.to_csv(f"{self.site_path}/meter_dp_df.csv", index=False)
        meter_dp_df = pd.read_csv(f"{self.site_path}/meter_dp_df.csv", index_col=False)
        # print(meter_dp_df[:2])
        if meter_dp_df.shape[0] == 0:
            self.logger.error(f"---- meter_dp_df is empty, so can't do migrate for the systems of {self.code} ----")   
            return None

        # sys_dp_df = pd.merge(sys_df, dp, how="left", left_on="id", right_on="ref_id")
        sys_meter_dp_df = pd.merge(left=sys_df, 
                    right=meter_dp_df, 
                    how='left',
                    left_on=['meter_id', 'source_key'], 
                    right_on=['meter_ref_id', 'source_key'])

        sys_meter_dp_df.to_csv(f"{self.site_path}/sys_meter_dp_df.csv", index=False)
        sys_meter_dp_df = pd.read_csv(f"{self.site_path}/sys_meter_dp_df.csv", index_col=False)
        # self.logger.info("---------------System with datapoint-----------------")
        # self.logger.info(sys_dp_df[:1])
        if sys_meter_dp_df.shape[0] == 0:
            self.logger.error(f"---- sys_meter_dp_df is empty, so can't do migrate for the systems of {self.code} ----")       
            return None     

        node_df = self.node.devices()
        node_df.to_csv(f"{self.site_path}/node_df.csv", index=False)
        node_df = pd.read_csv(f"{self.site_path}/node_df.csv", index_col=False)
        sys_meter_dp_node_df = pd.merge(left=sys_meter_dp_df, 
                    right=node_df, 
                    how='left',
                    left_on=['serial_device'], 
                    right_on=['node_device_serial'])

        result_df = pd.merge(sys_meter_dp_node_df, tenant_df, how="left", left_on="company_id", right_on="company_id")
        result_df.to_csv(f"{self.site_path}/result_df.csv", index=False)
        # self.logger.info("---------------System with datapoint, tenant-----------------")
        # self.logger.info(result_df[:8])        

        # release dataframe
        del tenant_df
        del sys_df
        del meters 
        del meter_dp_df
        del sys_meter_dp_df
        del sys_meter_dp_node_df
        del node_df

        return result_df
    
    def LoadData(self, filename=f"result_df.csv"):
        filepath = self.site_path + '/' + filename
        df = pd.read_csv(filepath, index_col=False)        
        # df = df.drop(['Unnamed: 0'], axis=1)
        return df

    def replace_expression_id(self, df, i):
        """
        循环替换 DataFrame 中的 {id_xxx}

        Args:
            df: 输入 DataFrame

        Returns:
            替换后的 DataFrame
        """        
        row = df.iloc[i]       
        system_id = row['id'] 
        old_composition_expression = row['composition_expression']
        new_composition_expression = row['composition_expression']
        self.logger.info("Replace old expression: {}".format(old_composition_expression))

        replace_ok = True
   
        id_list = []
        for match in re.finditer(r'{id_(\d+)}', old_composition_expression):
            id_xxx = match.group(1)
            node_ref_id = "xxxx"
            if df.loc[df['id'] == int(id_xxx), 'node_ref_id'].any():
                node_ref_id = df.loc[df['id'] == int(id_xxx), 'node_ref_id'].values[0]
            else:
                # Handle the case where no matching row is found (optional)
                id_list.append(str(id_xxx))
                # self.logger.error(f"not found id_{str(id_xxx)}")   
                replace_ok = False
                continue

            if replace_ok:
                str_node_ref_id = str(node_ref_id)
                if str_node_ref_id == 'unknown':
                    # Handle the case where component is sensor 
                    str_sensor_id = "sensor_id:"+str(id_xxx)
                    id_list.append(str_sensor_id)
                    # self.logger.error(f"not found id_{str(id_xxx)}")   
                    replace_ok = False
                    continue

                new_composition_expression = new_composition_expression.replace(match.group(0), "{id_"+str_node_ref_id+"}")
        

        if not replace_ok:            
            no_find_node_ref_id = list(set(id_list)) 
            self.logger.error(f"[Virtual] SysID [{system_id}]'s expression, their component id_xxx has no node_ref_id or including sensors, components id list: {no_find_node_ref_id}")
            self.logger.info("replace expression failed")       
            df.loc[i, 'composition_expression'] = old_composition_expression 
        else:
            self.logger.info("To replace expression with new: {}".format(new_composition_expression))    
            df.loc[i, 'composition_expression'] = new_composition_expression
            df.loc[i, 'new_expression'] = new_composition_expression        

        # self.logger.info("Replace expression with B: {}".format(composition_expression))

        return replace_ok

    def create_nodes_and_datapoints(self, sys_df, tenant_id, force_new=False):
        """
        根据能源系统的属性创建不同的节点和数据点，并建立它们之间的关系

        Args:
            sys_df: Pandas DataFrame，包含能源系统的信息

        Returns:
            None
        """
        sys_df["index"] = sys_df.index
        sys_df['use_datapoint_id'] = sys_df['dp_id']
        sys_df['use_system_id'] = np.nan #sys_df['ref_id']
        sys_df["node_id"] = 'unknown'        
        sys_df["node_type"] = 'unknown'       
        sys_df["data_type"] = 'unknown'     
        sys_df["node_ref_id"] = 'unknown'
        sys_df["old_expression"] = sys_df["composition_expression"]
        sys_df["new_expression"] = ''
        sys_df["component"] = 0
        sys_df["expression_replaced"] = 0
        sys_df["tenant_id"] = tenant_id

        self.logger.debug("======================= node devices =========================")        
        for i in range(len(sys_df)):
            if not is_none_or_nan(sys_df.loc[i, 'sensor_id']):
                if is_none_or_nan_zero(sys_df.loc[i, 'sensor_ptr_id']):
                    self.logger.debug(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] has sensor_id but no sensor_ptr_id and serial_device in sensor_iaqsensor table!!!") 
                    continue 
                if is_none_or_nan_zero(sys_df.loc[i, 'node_device_serial']):
                    self.logger.error(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] which no instance of node.devices!!!") 
                    continue 

                node_id, node_ref_id = None, None 
                # create new hierarchy node datapoint with data_type = 'TEMPERATURE', data_id = node.device.id
                # update dp_id for virtual system including sensor_ids?
                device_data_id = sys_df.loc[i, 'node_device_id'] 
                node_device_serial = sys_df.loc[i, 'node_device_serial'] 
                try:
                    if self.simulation:
                        node_id, node_ref_id = self.hr.create_simulate_node(node_type='DATAPOINT', 
                                                            data_type="TEMPERATURE", 
                                                            name=node_device_serial,                                                       
                                                            data_id = device_data_id,
                                                            desc = f"cid {sys_df.loc[i, 'company_id']} node.device serial {node_device_serial} device_id: {device_data_id}",  
                                                            ref_id=sys_df.loc[i, 'id'] 
                                                        )  
                    else:
                        node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                            data_type="TEMPERATURE", 
                                                            name=node_device_serial,
                                                            data_id = device_data_id,
                                                            desc = f"cid {sys_df.loc[i, 'company_id']} node.device serial {node_device_serial} device_id: {device_data_id}"
                                                        )    
                except Exception as e:
                        self.logger.error(f"create_node DATAPOINT err: {str(e)}")         
                        node_id, node_ref_id = self.hr.query_datapoint_node(data_id = device_data_id)  

                sys_df.loc[i, "node_type"] = 'DATAPOINT'       
                sys_df.loc[i, "node_id"] = node_id       
                sys_df.loc[i, "node_ref_id"] = node_ref_id  
                sys_df.loc[i, "data_type"] = 'TEMPERATURE'
                

        self.logger.debug("======================= site node =========================")
        # root system defined as hierachy.node_site
        for i in range(len(sys_df)):
            if is_none_or_nan_zero(sys_df.loc[i, 'parent_system_id']) \
                and is_none_or_nan(sys_df.loc[i, 'component_of_id']):                
                
                # city_name_df = sys_df.loc[sys_df["city_name"].notnull()] #sys_df["city_name"].iloc[0]
                # city_name = city_name_df["city_name"].iloc[0]
                city_name = sys_df.loc[i, 'city_name']                
                city = self.city.city(city_name)     
                city_id = zeroUUID()
                
                if city.shape[0] == 0:
                    self.logger.debug(f'''city: {city_name}, are not found the city id in city database by the city name!!! 
                        I will use Hong Kong's id as city id''')
                    city_name = 'Hong Kong'
                    city = self.city.city(city_name)
                    city_id = city['id'].iloc[0]
                    self.logger.debug(f'''city: {city_name}, id: {city_id}''')
                else:
                    city_id = city['id'].iloc[0]
                    self.logger.info(f'''city: {city_name}, id: {city_id}''')

                node_id = 0
                if self.simulation:
                    node_id = self.hr.create_simulate_node(name=sys_df.loc[i, 'system_name'], 
                                                city_id=city_id,
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                node_type='SITE',
                                                ref_id=sys_df.loc[i, 'id'])
                else:
                    node_id = self.hr.create_node(name=sys_df.loc[i, 'system_name'], 
                                                city_id=city_id,
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                node_type='SITE')

                sys_df.loc[i, "node_type"] = 'SITE'       
                sys_df.loc[i, "node_id"] = node_id

                self.logger.debug(f"[SITE] SysID [{sys_df.loc[i, 'id']}] [{sys_df.loc[i, 'system_name']}]")
                
        self.logger.debug("======================= pov node =========================")
        # folder system which has no meterid, defined as hierachy.node_pov
        for i in range(len(sys_df)):
            # if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']):
            #     ot = f"[POV] id [{sys_df.loc[i, 'id']}]"
            #     ce = str(sys_df.loc[i, 'composition_expression'])
            #     print(f"{ot} ce:{ce}, check-ce:{is_none_or_nan_zero(ce)}")
            #     mi = str(sys_df.loc[i, 'meter_id'])
            #     print(f"{ot} mi:{mi}, check-mi:{is_none_or_nan_zero(mi)}")

            if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']) \
                and is_none_or_nan_zero(str(sys_df.loc[i, 'composition_expression'])):
                # and is_none_or_nan_zero(str(sys_df.loc[i, 'meter_id'])):
                # and is_none_or_nan(sys_df.loc[i, 'source_key']):               

                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id']]
                # has child, this should be POV
                if child_df.shape[0] > 0: 
                    # somtetime it has configed to meterid or source key, it's not right config
                    mi = str(sys_df.loc[i, 'meter_id'])
                    if not is_none_or_nan_zero(mi):
                        self.logger.error(f"POV with meterID [{mi}] SysID:{sys_df.loc[i, 'id']}] System_name: [{sys_df.loc[i, 'system_name']}]")

                    sk = sys_df.loc[i, 'source_key']
                    if not is_none_or_nan_zero(sk):
                        self.logger.error(f"POV with sourcekey [{sys_df.loc[i, 'source_key']}] SysID:{sys_df.loc[i, 'id']}] System_name: [{sys_df.loc[i, 'system_name']}]")                                                
                else:
                    # if not POV, continue to next
                    continue
                

                node_id = 0

                if self.simulation:
                    node_id = self.hr.create_simulate_node(name=sys_df.loc[i, 'system_name'], 
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                node_type='POV',
                                                ref_id=sys_df.loc[i, 'id'])            
                else:
                    node_id = self.hr.create_node(name=sys_df.loc[i, 'system_name'], 
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                node_type='POV')            

                sys_df.loc[i, "node_type"] = 'POV'       
                sys_df.loc[i, "node_id"] = node_id        
                self.logger.debug(f"[POV] SysID [{sys_df.loc[i, 'id']}] [{sys_df.loc[i, 'system_name']}]")

        self.logger.debug("======================= none-virtual energy kwh system(node_data_points) =========================")
        # data system which has data(kwh), defined as hierachy.node_datapoint(data_type==energy) and energy.energy_datapoint                
        for i in range(len(sys_df)):
            if not is_none_or_nan(sys_df.loc[i, 'meter_id']) \
                and not is_none_or_nan_zero(sys_df.loc[i, 'source_key']) \
                    and is_none_or_nan_zero(str(sys_df.loc[i, 'composition_expression'])):                
                
                if is_none_or_nan_zero(sys_df.loc[i, 'dp_id']):
                    self.logger.error(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] has sourcekey and meterid, but it has no instance of energy.datapoint.id") 
                    continue

                if not is_none_or_nan_zero(sys_df.loc[i, 'ref_id']):
                    if sys_df.loc[i, 'ref_id'] != sys_df.loc[i, 'id']:
                        self.logger.debug(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] share with ref_id [{[{sys_df.loc[i, 'ref_id']}]}]") 


                # some system has meter_id and source_key and also has children systems, this is wrong config, print them out
                # and ignore them
                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id']]
                # has child, this should be POV
                if child_df.shape[0] > 0:                    
                    self.logger.error(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] with sourcekey and meterid, but it has children systems!!! wrong config.") 
                    continue                           
                

                _ref_id, _energy_datapoint_id = sys_df.loc[i, 'ref_id'], sys_df.loc[i, 'dp_id']  

                # 创建新的NODE, DATAPOINT类型的node
                node_id, node_ref_id = None, None                
                if sys_df.loc[i, "node_ref_id"] =='unknown':                
                    data_id = _energy_datapoint_id

                    try:
                        _source_key = sys_df.iloc[i]['source_key']
                        _meter_id = sys_df.iloc[i]['meter_id']

                        component_or_system = "system_" + str(sys_df.loc[i, 'id'])
                        if not is_none_or_nan(sys_df.loc[i, 'component_of_id']):
                            component_or_system = "component_system_" + str(sys_df.loc[i, 'id'])
              

                        if self.simulation:
                            node_id, node_ref_id = self.hr.create_simulate_node(node_type='DATAPOINT', 
                                                                data_type="ENERGY", 
                                                                name=sys_df.loc[i, 'system_name'],
                                                                #    desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                                # 如果是多个systems（同一个meterID，相同sourckey），虽然systemID不同，现在统一使用其中一个systemID
                                                                # 这样hierarchy.node_data_points.id 跟 energy.enerngy_datapoint.id就是1对1关系
                                                                # 实际情况是能找到多个systems都有enenrgy_datapoints记录，所以如果有此记录，则用它，没有的记录的，才取
                                                                # 第一个记录。另外多个这样的systems在node_data_points里边只有一个记录，既唯一对应node_data_points.ref_id
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} {component_or_system} mid {int(_meter_id)} sk: {_source_key}",
                                                                data_id = data_id,
                                                                ref_id=sys_df.loc[i, 'id'] )  
                        else:
                            node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                                data_type="ENERGY", 
                                                                name=sys_df.loc[i, 'system_name'],
                                                                #    desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",
                                                                # 如果是多个systems（同一个meterID，相同sourckey），虽然systemID不同，现在统一使用其中一个systemID
                                                                # 这样hierarchy.node_data_points.id 跟 energy.enerngy_datapoint.id就是1对1关系
                                                                # 实际情况是能找到多个systems都有enenrgy_datapoints记录，所以如果有此记录，则用它，没有的记录的，才取
                                                                # 第一个记录。另外多个这样的systems在node_data_points里边只有一个记录，既唯一对应node_data_points.ref_id
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} {component_or_system} mid {int(_meter_id)} sk: {_source_key}",
                                                                data_id = data_id)       
                    except Exception as e:
                        self.logger.error(f"create_node DATAPOINT err: {str(e)}")         
                        node_id, node_ref_id = self.hr.query_datapoint_node(data_id)                     
                    
                    # self.logger.debug(f"node_id:{node_id}, node_ref_id:{node_ref_id}")
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'       
                    sys_df.loc[i, "node_id"] = node_id       
                    sys_df.loc[i, "node_ref_id"] = node_ref_id  
                    sys_df.loc[i, "data_type"] = 'ENERGY'
                    sys_df.loc[i, "use_system_id"] = _ref_id
                    sys_df.loc[i, "use_datapoint_id"] = _energy_datapoint_id  
                    # self.logger.debug(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] NodeID: {node_id} Node_ref_id: {node_ref_id}")              
                    

                
                if not is_none_or_nan(sys_df.loc[i, 'component_of_id']):
                    # 如果是component system,它会在Hierarchy创建一个没有层级关系的datapoint
                    # 为了composition_expression能够使用node_ref_id,必须创建此datapoint
                    sys_df.loc[i, "component"] = 1       

                self.logger.debug(f"[ENERGY DATAPOINT] SysID [{sys_df.loc[i, 'id']}] [{sys_df.loc[i, 'system_name']}]")              

        self.logger.debug("======================= virtual system(node_data_points) =========================")
        # virtual system which has data(kwh) calculated by expression, it also take as data virtual system, 
        # so defined as hierachy.node_datapoint(data_type==virtual) and energy.energy_datapoint                
        pattern = re.compile(r"{id_(\d+)}")
        for i in range(len(sys_df)):
            if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']) \
                and not is_none_or_nan_zero(str(sys_df.loc[i, 'composition_expression'])):
                
                self.logger.debug(f"[Virtual] SysID [{sys_df.loc[i, 'id']}] [{sys_df.loc[i, 'system_name']}]")          
                
                composition_expression = str(sys_df.loc[i, 'composition_expression'])
                if '{' not in composition_expression and composition_expression!= '0':
                    self.logger.error(f"[Virtual] SysID [{sys_df.loc[i, 'id']}] it has no 'id_xxx' in composition_expression.") 
                    continue

                # some virtual system has children systems, this is wrong config, print them out
                # and ignore them
                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id']]
                # has child, this should be POV
                if child_df.shape[0] > 0:                    
                    self.logger.error(f"[Virtual] SysID [{sys_df.loc[i, 'id']}] but it has children systems!!! wrong config.") 
                    continue 

                # 更新composition_expression里边的id_xxx为'node_ref_id'
                replace_ok = True
                try:
                    replace_ok = self.replace_expression_id(sys_df, i)
                except Exception:
                    self.logger.error(f"replace expression failed, SysID:[{sys_df.loc[i, 'id']}]  expression: {sys_df.loc[i, 'composition_expression']}")
                    replace_ok = False
                    # self.logger.warning('''the expression didn't find any of id_xxx, id_xxx's system maybe are removed, 
                    #       update the expression to right, otherwise this virtual system will not create in hirachy.node_datapoint, 
                    #       then you can's see it in the front''')
                    
                    # continue
                
                if not replace_ok:
                    self.logger.error(f"[Virtual] SysID [{sys_df.loc[i, 'id']}] [{sys_df.loc[i, 'system_name']}] replace expression failed.")          
                    continue

                ################################
                ref_id = sys_df.loc[i, "ref_id"]
                energy_datapint_id = sys_df.loc[i, "dp_id"]
                str_tenant_id = str(sys_df.loc[i, "tenant_id"])
                tenant_id_bytes = small_endian_uuid(str_tenant_id)

                composition_expression = sys_df.loc[i, "composition_expression"]
                sys_df.loc[i, "expression_replaced"] = 1    

                virtualDatapointID = None
                if self.simulation:
                    virtualDatapointID = str(uuid.uuid4())             

                # 请求energy serice to create the relations of virtual_datapoint
                def RequestCreateVirtualDatapoint(kwargs):
                    stub = kwargs['stub']                                                                                                    
                    sid = sys_df.loc[i, 'id']        
                    refid_int64 = sid
                    
                    name = sys_df.loc[i, "system_name"]

                    self.logger.info(f"request create virtul datapoint for SysID:{sid} name:{name}")                        
                        
                    response = stub.PureCreate(vdpGrpcPb2.VirtualDatapoint(
                                                                    ref_id = refid_int64, 
                                                                    tenant_id = tenant_id_bytes,
                                                                    name = name,
                                                                    expression = composition_expression,
                                        ))                            

                    return response        

                # create new datapoint for virtual system
                if is_none_or_nan(ref_id):                                                       
                    self.logger.debug(f"not found energy_datapint_id for this virtual SysID:{sys_df.loc[i, 'id']}")
                    # 需要创建新的energy_datapint, enerng_virtual_datapint, energy_virtual_relation
                    # grpc request on 'Create' OF EnergyVirtualDatapoint service,
                    # before creating, need purge the old data in that 3 tables
                    # missing the datapoint.id, how to purge the 3 tables?
                    
                    # grpc request for updating energy_virtual_relationship                    
                    if not self.simulation:
                        response_data = run_grpc(stub=STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC, 
                                                        system_id = sys_df.loc[i, 'id'],
                                                        old_expression = sys_df.loc[i, 'old_expression'],
                                                        new_expression = sys_df.loc[i, 'new_expression'],
                                                        grpcFunction=RequestCreateVirtualDatapoint, 
                                                        logger=self.logger)
                        # response = vdpGrpcPb2.PureCreateResponse()
                        # response.ParseFromString(response_data)                        
                        # virtualDatapointID = response.virtual_datapoint_id                        
                        if response_data is None:
                            continue

                        serialized_data = response_data.SerializeToString()

                        response = vdpGrpcPb2.PureCreateResponse()
                        response.ParseFromString(serialized_data)
                        
                        uuid_virtualDatapointID = uuid.UUID(bytes=response.virtual_datapoint_id)
                        virtualDatapointID = str(uuid_virtualDatapointID)

                        sid = sys_df.loc[i, 'id']
                        self.logger.info(f"created virtul datapoint for SysID:{sid} virtualDatapointID:{uuid_virtualDatapointID}")

                        # use virtual datapoint.id -> datapoint.id -> set the ref_id to system.id
                        vdp_df = self.energy.getVirtualDataPointByID(virtualDatapointID)
                        datapoint_id = str(vdp_df['datapoint_id'].iloc[0])  
                        self.energy.updateRefIDofDataPoint(ref_id = int(sys_df.loc[i, "id"]), data_point_id = datapoint_id)

                        # 释放 dataframe 占用的内存
                        del vdp_df
                        # 强制垃圾回收器释放内存
                        gc.collect()
                   
                    
                    # energy_datapint_id set to virtualDatapointID
                    # as create new node_data_points need the data_id
                    # 1: 非virtual system
                    # energy.data_point.id = hierarchy.node_data_points.data_id
                    # 2: virtual system
                    # energy.virtual_datapoint.id = hierarchy.node_data_points.data_id
                    sys_df.loc[i, "use_system_id"] = sys_df.loc[i, "id"]
                    sys_df.loc[i, "use_datapoint_id"] = virtualDatapointID
                    sys_df.loc[i, "ref_id"] = sys_df.loc[i, "id"]
                    sys_df.loc[i, "dp_id"] = virtualDatapointID   
                    sys_df.loc[i, "data_type"] = 'VIRTUALDATAPOINT'                   
   
                else: 
                    
                    # 更新已经存在的virtual_datapoint里边的expression
                    # self.energy.update_virtual_datapoint(energy_datapint_id, composition_expression)  

                    # 请求energy serice to update the relations of virtual_datapoint
                    def RequestUpdateVirtualDatapoint(kwargs):
                        stub = kwargs['stub']
                        sid = sys_df.loc[i, 'id']                                                                         
                        refid_int64 = sid
                        vdp_df = self.energy.getVirtualDataPoint(energy_datapint_id, columns=["id", "status", "is_solar"])
                        if vdp_df is None:
                            self.logger.warning(f"no RequestUpdateRelations request as -> SysID:{sid} getVirtualDataPoint return empty")
                            # 释放 dataframe 占用的内存
                            del vdp_df

                            # 强制垃圾回收器释放内存
                            gc.collect()
                            return None
                        
                        id_of_virtual_datapoint = str(vdp_df['id'].iloc[0])       
                        virtualDatapointID = id_of_virtual_datapoint                  

                        dp_id = sys_df.loc[i, "dp_id"]
                        sid = sys_df.loc[i, 'id']
                        if is_none_or_nan(id_of_virtual_datapoint):                            
                            self.logger.warning(f"no RequestUpdateRelations request as -> SysID:{sid} energy_datapint_id:[{dp_id}] did not get virtual_datapoint id!!!")
                            del vdp_df
                            # 强制垃圾回收器释放内存
                            gc.collect()

                            return virtualDatapointID

                        # self.logger.info(f"SysID:{sid} energy_datapint_id:[{dp_id}] id_of_virtual_datapoint:[{id_of_virtual_datapoint}]")
                        
                        # id_of_virtual_datapoint = '16c85b2f-a614-45fb-91ff-6354c88e183b'
                        
                        # id_vd_bytes = id_of_virtual_datapoint.encode('utf-8')
                        id_vd_bytes = small_endian_uuid(id_of_virtual_datapoint)

                        # self.logger.info(f"--- test: id_of_virtual_datapoint [{id_of_virtual_datapoint}] bytes: {id_vd_bytes}")

                        # take the name of system as virtual datapoint name, maybe it's not equal to vdp_df['name'].iloc[0], use this name from ems
                        name = sys_df.loc[i, "system_name"] 
                        status = vdp_df['status'].iloc[0] # reference to VirtualDatapointStatus defined in energy_virtual_datapoint.proto
                        is_solar = vdp_df['is_solar'].iloc[0]
                        # datapoint_id = energy_datapint_id           
                        datapoint_id_bytes = small_endian_uuid(energy_datapint_id)

                        # ref_id = sys_df.loc[i, "ref_id"]
                        complex = 0 # reference to VirtualDatapointStatus defined in energy_virtual_datapoint.proto

                        import google.protobuf.wrappers_pb2 as wrappers
                        protobuf_bool = wrappers.BoolValue()
                        protobuf_bool.value = is_solar

                        response = stub.Update(vdpGrpcPb2.VirtualDatapoint(id = id_vd_bytes, 
                                                                        tenant_id = tenant_id_bytes,
                                                                        name = name,
                                                                        expression = composition_expression,
                                                                        status = status,
                                                                        is_solar = protobuf_bool,
                                                                        datapoint_id = datapoint_id_bytes,                                                                        
                                                                        ref_id = refid_int64, 
                                                                        complex = complex
                                                                        ))
                        del vdp_df

                        # 强制垃圾回收器释放内存
                        gc.collect()

                        self.logger.info(f"updated expression for SysID:{sid} energy_datapint_id:[{dp_id}] id_of_virtual_datapoint:[{id_of_virtual_datapoint}]")

                        sys_df.loc[i, "use_system_id"] = sys_df.loc[i, "id"]
                        sys_df.loc[i, "use_datapoint_id"] = virtualDatapointID
                        sys_df.loc[i, "ref_id"] = sys_df.loc[i, "id"]
                        sys_df.loc[i, "dp_id"] = virtualDatapointID  # 因为本来id_y对应的是energy.datapoint.id,但是这里是virtual system，所以需要替换成对应的energy.virtual_datapoint.id

                        return response
                    
                    # grpc request for updating energy_virtual_relationship
                    sid = sys_df.loc[i, 'id']
                    if not self.simulation:
                        create_new = False
                        vdp_df = self.energy.getVirtualDataPoint(energy_datapint_id, columns=["id", "status", "is_solar", "datapoint_id"])
                        
                        virtual_datapoint_id = None
                        if vdp_df.shape[0] != 0:
                            virtual_datapoint_id = str(vdp_df['id'].iloc[0])
                            node_dp_df = self.hr.nodeDataPointByDataID(data_id = virtual_datapoint_id)
                            if node_dp_df.shape[0] == 0:
                                create_new = True
                                self.logger.debug(f"Virtual sysID:{sid} has energy.virtual_datapoint instance but it has no data_id in hierarchy.node_datapoint.data_id, so need re-create it.")                            
                        else:
                            create_new = True
                            self.logger.debug(f"Virtual sysID:{sid} has no energy.virtual_datapoint instance. so need re-create it") 

                        # 释放 dataframe 占用的内存
                        del vdp_df
                        # 强制垃圾回收器释放内存
                        gc.collect()

                        if create_new:
                            sid = sys_df.loc[i, 'id']
                            
                            # 重现创建新的virtual datapoint instance
                            response_data = run_grpc(stub=STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC, 
                                                        system_id = sys_df.loc[i, 'id'],
                                                        old_expression = sys_df.loc[i, 'old_expression'],
                                                        new_expression = sys_df.loc[i, 'new_expression'],
                                                        grpcFunction=RequestCreateVirtualDatapoint, 
                                                        logger=self.logger)
                                              
                            if response_data is None:
                                continue

                            serialized_data = response_data.SerializeToString()

                            response = vdpGrpcPb2.PureCreateResponse()
                            response.ParseFromString(serialized_data)
                            
                            uuid_virtualDatapointID = uuid.UUID(bytes=response.virtual_datapoint_id)
                            virtualDatapointID = str(uuid_virtualDatapointID)

                            
                            self.logger.info(f"re-created virtul datapoint for SysID:{sid} virtualDatapointID:{uuid_virtualDatapointID}")

                            # use virtual datapoint.id -> datapoint.id -> set the ref_id to system.id
                            vdp_df = self.energy.getVirtualDataPointByID(virtualDatapointID)
                            datapoint_id = str(vdp_df['datapoint_id'].iloc[0])  
                            self.energy.updateRefIDofDataPoint(ref_id = int(sys_df.loc[i, "id"]), data_point_id = datapoint_id)

                            # 释放 dataframe 占用的内存
                            del vdp_df
                            # 强制垃圾回收器释放内存
                            gc.collect()

                            sys_df.loc[i, "use_system_id"] = sys_df.loc[i, "id"]
                            sys_df.loc[i, "use_datapoint_id"] = virtualDatapointID
                            sys_df.loc[i, "ref_id"] = sys_df.loc[i, "id"]
                            sys_df.loc[i, "dp_id"] = virtualDatapointID  
                            sys_df.loc[i, "data_type"] = 'VIRTUALDATAPOINT'  

                        else:                            
                            virtualDatapointID = virtual_datapoint_id
                            node_dp_df = self.hr.nodeDataPointByDataID(data_id = virtual_datapoint_id)
                            sid = sys_df.loc[i, 'id']

                            run_grpc(stub=STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC, 
                                                    system_id = sys_df.loc[i, 'id'],
                                                    old_expression = sys_df.loc[i, 'old_expression'],
                                                    new_expression = sys_df.loc[i, 'new_expression'],
                                                    grpcFunction=RequestUpdateVirtualDatapoint, 
                                                    logger=self.logger )  
                            
                            node_id = node_dp_df['id'].iloc[0]
                            node_ref_id = node_dp_df['ref_id'].iloc[0]  

                            sys_df.loc[i, "node_type"] = 'DATAPOINT'
                            sys_df.loc[i, "node_id"] = node_id
                            sys_df.loc[i, "node_ref_id"] = node_ref_id                   
                            sys_df.loc[i, "data_type"] = 'VIRTUALDATAPOINT'  

                            # use virtual datapoint.id -> datapoint.id -> set the ref_id to system.id
                            vdp_df = self.energy.getVirtualDataPointByID(virtualDatapointID)
                            datapoint_id = str(vdp_df['datapoint_id'].iloc[0])  
                            self.energy.updateRefIDofDataPoint(ref_id = int(sys_df.loc[i, "id"]), data_point_id = datapoint_id)

                            # 释放 dataframe 占用的内存
                            del vdp_df
                            # 强制垃圾回收器释放内存
                            gc.collect() 

                            self.logger.info(f"updated node for SysID:{sid} node_id:{node_id} node_ref_id:{node_ref_id}")
                        
                            continue
                        

                    
                # generate new VIRTUALDATAPOINT node on hierarchy
                if sys_df.loc[i, "node_ref_id"] =='unknown' \
                    and not is_none_or_nan(virtualDatapointID) \
                    and replace_ok:  

                    node_id, node_ref_id = None, None
                    if self.simulation:                           
                        node_id, node_ref_id = self.hr.create_simulate_node(node_type='DATAPOINT', 
                                                                data_type="VIRTUALDATAPOINT", 
                                                                name=sys_df.loc[i, 'system_name'], 
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",                                                            
                                                                data_id=virtualDatapointID,
                                                                ref_id=sys_df.loc[i, 'id'])                
                    else:
                        node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                            data_type="VIRTUALDATAPOINT", 
                                                            name=sys_df.loc[i, 'system_name'], 
                                                            desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id']}",                                                            
                                                            data_id=virtualDatapointID)                
                            
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'
                    sys_df.loc[i, "node_id"] = node_id
                    sys_df.loc[i, "node_ref_id"] = node_ref_id                    
                    sys_df.loc[i, "data_type"] = 'VIRTUALDATAPOINT'   

                    self.logger.info(f"created node for SysID:{sid} node_id:{node_id} node_ref_id:{node_ref_id}")

        
    

        # 创建父子节点关系
        # 根据id_x, parent_system_id 关系，在hierarchy 的 relatives里边创建 父子关系          
        if not self.simulation:
            self.logger.info("--------------- create_relations in hierarchy -----------------")
            self.hr.create_relations(sys_df, components_binding = self.components_binding)
            self.hr.simulation_sys_df = sys_df
        else:
            self.logger.info("--------------- simulate create_relations in hierarchy -----------------")       
            self.hr.create_relations(sys_df, components_binding = self.components_binding)     
            self.hr.simulation_sys_df = sys_df

        
        sys_df.to_csv(f"{self.site_path}/sys_df_after.csv", index=False)




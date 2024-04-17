import pandas as pd
import numpy as np
import pandas as pd
from EMS import EMS
from Energy import Energy
from Hierarchy import Hierarchy
from Organization import Organization
from City import City
import xml.etree.ElementTree as ET
import re
from common import logger, zeroUUID, is_none_or_nan, is_none_or_nan_zero, readOption, run_grpc, big_endian_uuid, STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC
import energy_virtual_datapoint_pb2 as vdpGrpcPb2 
import uuid
import gc
from pathlib import Path
import os


class DataFlow(object):
    simulation = False
    code = 'demohk'

    def __init__(self, code = None, components_binding=False, simulation = False):     
        self.simulation = simulation        
        self.code = code
        site_path = f"./output/{self.code}"
        if not Path(f"{site_path}").is_dir(): 
            os.makedirs(site_path)

        host=readOption("databases.hierarchy.host")
        port=readOption("databases.hierarchy.port")
        database=readOption("databases.hierarchy.database")
        user=readOption("databases.hierarchy.username")
        password=readOption("databases.hierarchy.password")

        self.hr = Hierarchy(host=host, port=port, user=user, password=password, database=database, simulation=simulation)
        
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
        

        self.code = code
        self.components_binding = components_binding
    
    
    def Building(self):
        df = self.PreparingData()
        # self.create_nodes_and_datapoints(df)
    
    
    def PreparingData(self):       
        
        site_path = f"./output/{self.code}"      

        company_df = self.ems.company(code=self.code)
        tenant_df = self.org.Tenant(code=self.code)
        joined_tenant = pd.merge(company_df, tenant_df, how="left", left_on="code", right_on="company_code")
        tenant_df = joined_tenant[['id_x', 'id_y', 'name_x', 'code']]        

        new_names = {'id_x': 'company_id', 'id_y': 'tenant_id', 'name_x': 'company', "code": 'company_code'}
        tenant_df = tenant_df.rename(columns=new_names)
        # tenant_df.to_csv(f"{site_path}/tenant_df.csv")

        # logger.info("---------------Tenant-----------------")
        # logger.info(tenant_df[:3])

        sys_df = self.ems.systems(code=self.code)
        # sys_df.to_csv(f"{site_path}/sys_df.csv")

        dp = self.energy.dataPoint(columns = ["id", "ref_id", "name"])
        # dp.to_csv(f"{site_path}/dp.csv")

        sys_dp_df = pd.merge(sys_df, dp, how="left", left_on="id", right_on="ref_id")
        # sys_dp_df.to_csv(f"{site_path}/sys_dp_df.csv")
        # logger.info("---------------System with datapoint-----------------")
        # logger.info(sys_dp_df[:1])

        result_df = pd.merge(sys_dp_df, tenant_df, how="left", left_on="company_id", right_on="company_id")
        # result_df.to_csv(f"{site_path}/result_df.csv")
        # logger.info("---------------System with datapoint, tenant-----------------")
        # logger.info(result_df[:8])

        return result_df
    
    def LoadData(self, filename="{site_path}/result_df.csv"):
        df = pd.read_csv(filename, index_col=False)        
        df = df.drop(['Unnamed: 0'], axis=1)
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
        composition_expression = row['composition_expression']
        logger.info("Replace expression A: {}".format(composition_expression))

        for match in re.finditer(r'{id_(\d+)}', composition_expression):
            id_xxx = match.group(1)
            node_ref_id = df.loc[df['id_x'] == int(id_xxx), 'node_ref_id'].values[0]
            composition_expression = composition_expression.replace(match.group(0), "{id_"+str(node_ref_id)+"}")

        logger.info("To replace expression with B: {}".format(composition_expression))    
        df.loc[i, 'composition_expression'] = composition_expression        

        # logger.info("Replace expression with B: {}".format(composition_expression))

        return df

    def create_nodes_and_datapoints(self, sys_df, tenant_id, force_new=False):
        """
        根据能源系统的属性创建不同的节点和数据点，并建立它们之间的关系

        Args:
            sys_df: Pandas DataFrame，包含能源系统的信息

        Returns:
            None
        """
        sys_df["index"] = sys_df.index
        sys_df['use_datapoint_id'] = sys_df['id_y']
        sys_df['use_system_id'] = np.nan #sys_df['ref_id']
        sys_df["node_id"] = 'unknown'        
        sys_df["node_type"] = 'unknown'       
        sys_df["data_type"] = 'unknown'     
        sys_df["node_ref_id"] = 'unknown'
        sys_df["component"] = 0
        sys_df["expression_replaced"] = 0
        sys_df["tenant_id"] = tenant_id

        logger.debug("======================= site node =========================")
        # root system defined as hierachy.node_site
        for i in range(len(sys_df)):
            if np.isnan(sys_df.loc[i, 'parent_system_id']) \
                and is_none_or_nan(sys_df.loc[i, 'component_of_id']):                
                
                # city_name_df = sys_df.loc[sys_df["city_name"].notnull()] #sys_df["city_name"].iloc[0]
                # city_name = city_name_df["city_name"].iloc[0]
                city_name = sys_df.loc[i, 'city_name']                
                city = self.city.city(city_name)     
                city_id = zeroUUID()
                
                if city.shape[0] == 0:
                    logger.warning(f'''city: {city_name}, are not found the city id in city database by the city name!!! 
                        I will use Hong Kong's id as city id''')
                    city_name = 'Hong Kong'
                    city = self.city.city(city_name)
                    city_id = city['id'].iloc[0]
                    logger.debug(f'''city: {city_name}, id: {city_id}''')
                else:
                    city_id = city['id'].iloc[0]
                    logger.info(f'''city: {city_name}, id: {city_id}''')

                node_id = 0
                if self.simulation:
                    node_id = self.hr.create_simulate_node(name=sys_df.loc[i, 'name_x'], 
                                                city_id=city_id,
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                node_type='SITE',
                                                ref_id=sys_df.loc[i, 'id_x'])
                else:
                    node_id = self.hr.create_node(name=sys_df.loc[i, 'name_x'], 
                                                city_id=city_id,
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                node_type='SITE')

                sys_df.loc[i, "node_type"] = 'SITE'       
                sys_df.loc[i, "node_id"] = node_id

                logger.debug(f"[SITE] ===== ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")
                
        logger.debug("======================= pov node =========================")
        # folder system which has no meterid, defined as hierachy.node_pov
        for i in range(len(sys_df)):
            # if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']):
            #     ot = f"[POV] id [{sys_df.loc[i, 'id_x']}]"
            #     ce = str(sys_df.loc[i, 'composition_expression'])
            #     print(f"{ot} ce:{ce}, check-ce:{is_none_or_nan_zero(ce)}")
            #     mi = str(sys_df.loc[i, 'meter_id'])
            #     print(f"{ot} mi:{mi}, check-mi:{is_none_or_nan_zero(mi)}")

            if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']) \
                and is_none_or_nan_zero(str(sys_df.loc[i, 'composition_expression'])):
                # and is_none_or_nan_zero(str(sys_df.loc[i, 'meter_id'])):
                # and is_none_or_nan(sys_df.loc[i, 'source_key']):               

                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id_x']]
                # has child, this should be POV
                if child_df.shape[0] > 0: 
                    # somtetime it has configed to meterid or source key, it's not right config
                    mi = str(sys_df.loc[i, 'meter_id'])
                    if not is_none_or_nan_zero(mi):
                        logger.warning(f"POV has meterID [{mi}], ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")

                    sk = sys_df.loc[i, 'source_key']
                    if not is_none_or_nan_zero(sk):
                        logger.warning(f"POV has sourcekey [{sys_df.loc[i, 'source_key']}], ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")                                                
                else:
                    # if not POV, continue to next
                    continue
                

                node_id = 0

                if self.simulation:
                    node_id = self.hr.create_simulate_node(name=sys_df.loc[i, 'name_x'], 
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                node_type='POV',
                                                ref_id=sys_df.loc[i, 'id_x'])            
                else:
                    node_id = self.hr.create_node(name=sys_df.loc[i, 'name_x'], 
                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                node_type='POV')            

                sys_df.loc[i, "node_type"] = 'POV'       
                sys_df.loc[i, "node_id"] = node_id        
                logger.debug(f"[POV] ===== ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")

        logger.debug("======================= none-virtual energy system(node_data_points) =========================")
        # data system which has data(kwh), defined as hierachy.node_datapoint(data_type==energy) and energy.energy_datapoint                
        for i in range(len(sys_df)):
            if not is_none_or_nan(sys_df.loc[i, 'meter_id']) \
                and not is_none_or_nan_zero(sys_df.loc[i, 'source_key']):                
                # some system has meter_id and source_key and also has children systems, this is wrong config, print them out
                # and ignore them
                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id_x']]
                # has child, this should be POV
                if child_df.shape[0] > 0:                    
                    logger.error(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] with sourcekey and meterid, but it has children systems!!! wrong config.") 
                    continue                


                meter_id = sys_df.loc[i, 'meter_id']
                source_key = sys_df.loc[i, 'source_key']
                sys_name = sys_df.loc[i, 'name_x']

                _df = sys_df[(sys_df['meter_id']==meter_id) & \
                            (sys_df['source_key']==source_key) & \
                            (sys_df['ref_id']!=np.nan) ]
                
                _ref_id, _energy_datapoint_id = None, None
                if _df.shape[0] == 0:
                    # 说明此sourcekey对应的所有systems都没有一个在energy.datapoint中出现
                    # 创建一个新的energy.datapoint
                    _ref_id = sys_df.loc[i, 'id_x']
                    name = sys_df.loc[i, "name_x"]
                    meter_id = sys_df.loc[i, "meter_id"]

                    if force_new:
                        # TODO
                        # NEED CREATE METER IN ENERGY IF NOT FOUND IT
                        # ...................................

                        # 数据节点用sourcekey作为它的name，如果是虚拟数据节点，因为没有sourckey，只能用system name作为它的name
                        _energy_datapoint_id = self.energy.create_new_datapoint(_ref_id, source_key, meter_id) 
                    else:
                        logger.warning(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] not found kwh system:[{_ref_id}] name:[{name}] in energy.datapoint")

                if _df.shape[0] >= 1:
                    # print(_df) parent_system_id/meter_id/use_system_id/.. are 1125.0 has xxx.0,need fix?
                    # 说明此meter的source_key下有多个systems，其中一个在energy.datapoint已经配置
                    # 更新所有此meter,source_key对应的systems，让它们使用同一个energy.datapoint的记录（ID）
                    
                    # _df['id_y'] 有一些值是nan,有些值是34.0, 当我用_df[(~np.isnan(_df['id_y']))]时候报错，
                    # TypeError: ufunc 'isnan' not supported
                    # _df['id_y'] = pd.to_numeric(_df['id_y'])
                    # # 此时可以使用np.isnan()函数判断是否为nan
                    # _df[(~np.isnan(_df['id_y']))]
                    # 或则用pd.isnull(_df['id_y'])

                    _df2 = _df[(~pd.isnull(_df['id_x'])) & (~pd.isnull(_df['id_y']))]            

                    if _df2.shape[0] == 0:
                        logger.warning(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] meter:[{int(meter_id)}] source_key:[{source_key}] name: [{sys_name}] dosnt find energy_datapoint.id")
                        _energy_datapoint_id = None
                        _ref_id = None
                        # 当前这个system在energy_datapoint里边没找到记录，
                        # 则取相同meterid和sourcekey的system对应的第一个energy_datapoint的记录   
                        _df3 = sys_df[(sys_df['meter_id']==meter_id) & \
                            (sys_df['source_key']==source_key)] 
                    
                        for _, row in _df3.iterrows():
                            if not is_none_or_nan(row['id_y']):
                                _energy_datapoint_id = row['id_y']
                                _ref_id = row['ref_id']
                                logger.info(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] will use sysID:[{_ref_id}] with energy_datapoint.id:{_energy_datapoint_id}")
                                break
                        
                        if _energy_datapoint_id is None:
                            logger.warning(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] didn't find any energy.datapoint_id for replace it.")
                        
                        del _df3
                        

                    if _df2.shape[0] >= 1:                           
                        # 同一个meter，同一个sourckey在energy_datapoint里有可能存在多个记录
                        # 取当前对应的energy.datapoint.id
                        _ref_id = sys_df.iloc[i]['id_x']
                        _energy_datapoint_id = sys_df.iloc[i]['id_y']

                    del _df2
                        
                        

                # 创建新的NODE, DATAPOINT类型的node
                node_id, node_ref_id = None, None                
                if sys_df.loc[i, "node_ref_id"] =='unknown' and _energy_datapoint_id is not None:                                    
                    data_id = _energy_datapoint_id #sys_df.loc[i, "id_y"]                    

                    try:
                        _source_key = sys_df.iloc[i]['source_key']
                        _meter_id = sys_df.iloc[i]['meter_id']

                        if self.simulation:
                            node_id, node_ref_id = self.hr.create_simulate_node(node_type='DATAPOINT', 
                                                                data_type="ENERGY", 
                                                                name=sys_df.loc[i, 'name_x'],
                                                                #    desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                                # 如果是多个systems（同一个meterID，相同sourckey），虽然systemID不同，现在统一使用其中一个systemID
                                                                # 这样hierarchy.node_data_points.id 跟 energy.enerngy_datapoint.id就是1对1关系
                                                                # 实际情况是能找到多个systems都有enenrgy_datapoints记录，所以如果有此记录，则用它，没有的记录的，才取
                                                                # 第一个记录。另外多个这样的systems在node_data_points里边只有一个记录，既唯一对应node_data_points.ref_id
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} mid {_meter_id} sk: {_source_key}",
                                                                data_id = data_id,
                                                                ref_id=sys_df.loc[i, 'id_x'] )  
                        else:
                            node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                                data_type="ENERGY", 
                                                                name=sys_df.loc[i, 'name_x'],
                                                                #    desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                                # 如果是多个systems（同一个meterID，相同sourckey），虽然systemID不同，现在统一使用其中一个systemID
                                                                # 这样hierarchy.node_data_points.id 跟 energy.enerngy_datapoint.id就是1对1关系
                                                                # 实际情况是能找到多个systems都有enenrgy_datapoints记录，所以如果有此记录，则用它，没有的记录的，才取
                                                                # 第一个记录。另外多个这样的systems在node_data_points里边只有一个记录，既唯一对应node_data_points.ref_id
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} mid {_meter_id} sk: {_source_key}",
                                                                data_id = data_id)       
                    except Exception as e:
                        logger.error(f"create_node DATAPOINT err: {str(e)}")         
                        node_id, node_ref_id = self.hr.query_datapoint_node(data_id)                     
                    
                    # logger.debug(f"node_id:{node_id}, node_ref_id:{node_ref_id}")
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'       
                    sys_df.loc[i, "node_id"] = node_id       
                    sys_df.loc[i, "node_ref_id"] = node_ref_id  
                    sys_df.loc[i, "data_type"] = 'ENERGY'       
     
                # 更新
                if _ref_id is not None:
                    sys_df.loc[i, "use_system_id"] = _ref_id
                    sys_df.loc[i, "use_datapoint_id"] = _energy_datapoint_id  
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'       
                    sys_df.loc[i, "node_id"] = node_id       
                    sys_df.loc[i, "node_ref_id"] = node_ref_id  
                    sys_df.loc[i, "data_type"] = 'ENERGY'       

                
                if not is_none_or_nan(sys_df.loc[i, 'component_of_id']):
                    # 如果是component system,它会在Hierarchy创建一个没有层级关系的datapoint
                    # 为了composition_expression能够使用node_ref_id,必须创建此datapoint
                    sys_df.loc[i, "component"] = 1       

                logger.debug(f"[ENERGY DATAPOINT] ===== ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")              

        logger.debug("======================= virtual system(node_data_points) =========================")
        # virtual system which has data(kwh) calculated by expression, it also take as data virtual system, 
        # so defined as hierachy.node_datapoint(data_type==virtual) and energy.energy_datapoint                
        pattern = re.compile(r"{id_(\d+)}")
        for i in range(len(sys_df)):
            if not is_none_or_nan(sys_df.loc[i, 'parent_system_id']) \
                and not is_none_or_nan(sys_df.loc[i, 'composition_expression']):
                
                if '{' not in str(sys_df.loc[i, 'composition_expression']):
                    logger.warning(f"[Virtual] ===== ID [{sys_df.loc[i, 'id_x']}] it has no composition_expression.") 
                    continue
                
                # some virtual system has children systems, this is wrong config, print them out
                # and ignore them
                child_df = sys_df[sys_df['parent_system_id'] == sys_df.loc[i, 'id_x']]
                # has child, this should be POV
                if child_df.shape[0] > 0:                    
                    logger.error(f"[Virtual] ===== ID [{sys_df.loc[i, 'id_x']}] but it has children systems!!! wrong config.") 
                    continue 

                # 更新composition_expression里边打id_xxx为'node_ref_id'
                try:
                    self.replace_expression_id(sys_df, i)
                except Exception:
                    logger.error(f"------ call replace_expression_id failed, expression: {sys_df.loc[i, 'composition_expression']} ------")
                    # logger.warning('''the expression didn't find any of id_xxx, id_xxx's system maybe are removed, 
                    #       update the expression to right, otherwise this virtual system will not create in hirachy.node_datapoint, 
                    #       then you can's see it in the front''')
                    
                    # continue
                
                ################################
                ref_id = sys_df.loc[i, "ref_id"]
                energy_datapint_id = sys_df.loc[i, "id_y"]
                tenant_id = sys_df.loc[i, "tenant_id"]
                composition_expression = sys_df.loc[i, "composition_expression"]
                sys_df.loc[i, "expression_replaced"] = 1                

                # create new datapoint for virtual system
                if is_none_or_nan(ref_id):                                                       
                    logger.warning(f"not found energy_datapint_id for this virtual systemID:{sys_df.loc[i, 'id_x']}")
                    # 需要创建新的energy_datapint, enerng_virtual_datapint, energy_virtual_relation
                    # grpc request on 'Create' OF EnergyVirtualDatapoint service,
                    # before creating, need purge the old data in that 3 tables
                    # missing the datapoint.id, how to purge the 3 tables?
                    

                    # 请求energy serice to create the relations of virtual_datapoint
                    def RequestCreateVirtualDatapoint(kwargs):
                        stub = kwargs['stub']                                                                                    
                
                        sid = sys_df.loc[i, 'id_x']                    

                        logger.info(f"create virtul datapoint for systemID:{sid} ")
                        
                        name = sys_df.loc[i, "name_x"] 
                        response = stub.Create(vdpGrpcPb2.VirtualDatapoint(
                                                                        tenant_id = tenant_id.encode(),
                                                                        name = name,
                                                                        expression = composition_expression,
                                            ))                            

                        return response
                    
                    # grpc request for updating energy_virtual_relationship
                    virtualDatapointID = str(uuid.uuid4())
                    if not self.simulation:
                        virtualDatapointID = run_grpc(stub=STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC, grpcFunction=RequestCreateVirtualDatapoint )
                   
                    
                    # energy_datapint_id set to virtualDatapointID
                    # as create new node_data_points need the data_id
                    # 1: 非virtual system
                    # energy.data_point.id = hierarchy.node_data_points.data_id
                    # 2: virtual system
                    # energy.virtual_datapoint.id = hierarchy.node_data_points.data_id
                    energy_datapint_id = virtualDatapointID        

                    sys_df.loc[i, "use_system_id"] = sys_df.loc[i, "id_x"]
                    sys_df.loc[i, "use_datapoint_id"] = energy_datapint_id # this is used virtualDatapointID
                    sys_df.loc[i, "ref_id"] = sys_df.loc[i, "id_x"]
                    sys_df.loc[i, "id_y"] = energy_datapint_id                    
   
                else:

                    # 更新已经存在的virtual_datapoint里边的expression
                    # self.energy.update_virtual_datapoint(energy_datapint_id, composition_expression)  

                    # 请求energy serice to update the relations of virtual_datapoint
                    def RequestUpdateVirtualDatapoint(kwargs):
                        stub = kwargs['stub']
                        vdp_df = self.energy.getVirtualDataPoint(energy_datapint_id, columns=["id", "status", "is_solar"])
                        if vdp_df is None:
                            logger.warning(f"no RequestUpdateRelations request as -> systemID:{sid} getVirtualDataPoint return empty")
                            # 释放 dataframe 占用的内存
                            del vdp_df

                            # 强制垃圾回收器释放内存
                            gc.collect()
                            return None
                        
                        id_of_virtual_datapoint = str(vdp_df['id'].iloc[0])
                        edid = sys_df.loc[i, "id_y"]
                        sid = sys_df.loc[i, 'id_x']
                        if is_none_or_nan(id_of_virtual_datapoint):                            
                            logger.warning(f"no RequestUpdateRelations request as -> systemID:{sid} energy_datapint_id:[{edid}] did not get virtual_datapoint id!!!")
                            del vdp_df
                            # 强制垃圾回收器释放内存
                            gc.collect()

                            return None

                        logger.info(f"systemID:{sid} energy_datapint_id:[{edid}] id_of_virtual_datapoint:[{id_of_virtual_datapoint}]")
                        
                        # id_of_virtual_datapoint = '16c85b2f-a614-45fb-91ff-6354c88e183b'
                        
                        # id_vd_bytes = id_of_virtual_datapoint.encode('utf-8')
                        id_vd_bytes = big_endian_uuid(id_of_virtual_datapoint)

                        # logger.info(f"--- test: id_of_virtual_datapoint [{id_of_virtual_datapoint}] bytes: {id_vd_bytes}")

                        # take the name of system as virtual datapoint name, maybe it's not equal to vdp_df['name'].iloc[0], use this name from ems
                        name = sys_df.loc[i, "name_x"] 
                        status = vdp_df['status'].iloc[0] # reference to VirtualDatapointStatus defined in energy_virtual_datapoint.proto
                        is_solar = vdp_df['is_solar'].iloc[0]
                        datapoint_id = energy_datapint_id                        
                        ref_id = sys_df.loc[i, "ref_id"]
                        complex = 0 # reference to VirtualDatapointStatus defined in energy_virtual_datapoint.proto

                        import google.protobuf.wrappers_pb2 as wrappers
                        protobuf_bool = wrappers.BoolValue()
                        protobuf_bool.value = is_solar

                        response = stub.Update(vdpGrpcPb2.VirtualDatapoint(id = id_vd_bytes, 
                                                                        tenant_id = tenant_id.encode(),
                                                                        name = name,
                                                                        expression = composition_expression,
                                                                        status = status,
                                                                        is_solar = protobuf_bool,
                                                                        datapoint_id = datapoint_id.encode(),
                                                                        ref_id = int(ref_id),
                                                                        complex = complex
                                                                        ))
                        del vdp_df

                        # 强制垃圾回收器释放内存
                        gc.collect()

                        return response
                    
                    # grpc request for updating energy_virtual_relationship

                    if not self.simulation:
                        run_grpc(stub=STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC, grpcFunction=RequestUpdateVirtualDatapoint )  

                    

                if sys_df.loc[i, "node_ref_id"] =='unknown' and not is_none_or_nan(energy_datapint_id):  

                    if self.simulation:                           
                        node_id, node_ref_id = self.hr.create_simulate_node(node_type='DATAPOINT', 
                                                                data_type="VIRTUALDATAPOINT", 
                                                                name=sys_df.loc[i, 'name_x'], 
                                                                desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",                                                            
                                                                data_id=energy_datapint_id,
                                                                ref_id=sys_df.loc[i, 'id_x'])                
                    else:
                        node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                            data_type="VIRTUALDATAPOINT", 
                                                            name=sys_df.loc[i, 'name_x'], 
                                                            desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",                                                            
                                                            data_id=energy_datapint_id)                
                            
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'
                    sys_df.loc[i, "node_id"] = node_id
                    sys_df.loc[i, "node_ref_id"] = node_ref_id                    
                    sys_df.loc[i, "data_type"] = 'VIRTUALDATAPOINT'   

                logger.debug(f"[Virtual] ===== ID [{sys_df.loc[i, 'id_x']}] [{sys_df.loc[i, 'name_x']}]")                 

        
    

        # 创建父子节点关系
        # 根据id_x, parent_system_id 关系，在hierarchy 的 relatives里边创建 父子关系          
        if not self.simulation:
            logger.info("--------------- create_relations in hierarchy -----------------")
            self.hr.create_relations(sys_df, components_binding = self.components_binding)
            self.hr.simulation_sys_df = sys_df
        else:
            logger.info("--------------- simulate create_relations in hierarchy -----------------")       
            self.hr.create_relations(sys_df, components_binding = self.components_binding)     
            self.hr.simulation_sys_df = sys_df

        site_path = f"./output/{self.code}"
        sys_df.to_csv(f"{site_path}/sys_df_after.csv")

        
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
from common import zeroUUID, is_none_or_nan, readOption

class DataFlow(object):

    def __init__(self, code = None, components_binding=False):     
        host=readOption("databases.hierarchy.host")
        port=readOption("databases.hierarchy.port")
        database=readOption("databases.hierarchy.database")
        user=readOption("databases.hierarchy.username")
        password=readOption("databases.hierarchy.password")

        self.hr = Hierarchy(host=host, port=port, user=user, password=password, database=database)
        
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
        company_df = self.ems.company(code=self.code)
        tenant_df = self.org.Tenant(code=self.code)
        joined_tenant = pd.merge(company_df, tenant_df, how="left", left_on="code", right_on="company_code")
        tenant_df = joined_tenant[['id_x', 'id_y', 'name_x', 'code']]        

        new_names = {'id_x': 'company_id', 'id_y': 'tenant_id', 'name_x': 'company', "code": 'company_code'}
        tenant_df = tenant_df.rename(columns=new_names)
        tenant_df.to_csv(f"./output/tenant_df.csv")

        # print("---------------Tenant-----------------")
        # print(tenant_df[:3])

        sys_df = self.ems.systems(code=self.code)
        sys_df.to_csv(f"./output/sys_df.csv")

        dp = self.energy.dataPoint(columns = ["id", "ref_id", "name"])
        dp.to_csv(f"./output/dp.csv")

        sys_dp_df = pd.merge(sys_df, dp, how="left", left_on="id", right_on="ref_id")
        sys_dp_df.to_csv(f"./output/sys_dp_df.csv")
        # print("---------------System with datapoint-----------------")
        # print(sys_dp_df[:1])

        result_df = pd.merge(sys_dp_df, tenant_df, how="left", left_on="company_id", right_on="company_id")
        result_df.to_csv(f"./output/result_df.csv")
        # print("---------------System with datapoint, tenant-----------------")
        # print(result_df[:8])

        return result_df

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
        print("-------------A---------------")
        print(f"c1: {composition_expression}")
        for match in re.finditer(r'{id_(\d+)}', composition_expression):
            id_xxx = match.group(1)
            node_ref_id = df.loc[df['id_x'] == int(id_xxx), 'node_ref_id'].values[0]
            composition_expression = composition_expression.replace(match.group(0), "{id_"+str(node_ref_id)+"}")
        df.loc[i, 'composition_expression'] = composition_expression
        print(f"c2: {composition_expression}")
        print("-------------B---------------")

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
        sys_df["node_ref_id"] = 'unknown'
        sys_df["component"] = 0
        sys_df["expression_replaced"] = 0
        sys_df["tenant_id"] = tenant_id

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
                    print(f'''city: {city_name}, are not found the city id in city database by the city name!!! 
                        I will use Hong Kong's id as city id''')
                    city_name = 'Hong Kong'
                    city = self.city.city(city_name)
                    city_id = city['id'].iloc[0]
                    print(f'''city: {city_name}, id: {city_id}''')
                else:
                    city_id = city['id'].iloc[0]
                    print(f'''city: {city_name}, id: {city_id}''')

                node_id = self.hr.create_node(name=sys_df.loc[i, 'name_x'], 
                                              city_id=city_id,
                                              desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                              node_type='SITE')
                sys_df.loc[i, "node_type"] = 'SITE'       
                sys_df.loc[i, "node_id"] = node_id
                
        
        # folder system which has no meterid, defined as hierachy.node_pov
        for i in range(len(sys_df)):
            if not np.isnan(sys_df.loc[i, 'parent_system_id']) \
                and sys_df.loc[i, 'composition_expression'] is None \
                and np.isnan(sys_df.loc[i, 'meter_id']) \
                and (sys_df.loc[i, 'source_key'] is None or len(sys_df.loc[i, 'source_key'])==0):
                # 如果meter_id是空打，而sourckey非空，为脏数据，可以ignore

                node_id = self.hr.create_node(name=sys_df.loc[i, 'name_x'], 
                                              desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                              node_type='POV')                
                sys_df.loc[i, "node_type"] = 'POV'       
                sys_df.loc[i, "node_id"] = node_id        

        # data system which has data(kwh), defined as hierachy.node_datapoint and energy.energy_datapoint                
        for i in range(len(sys_df)):
            if not np.isnan(sys_df.loc[i, 'meter_id']) \
                and (sys_df.loc[i, 'source_key'] is not None and len(sys_df.loc[i, 'source_key']) > 0):

                meter_id = sys_df.loc[i, 'meter_id']
                source_key = sys_df.loc[i, 'source_key']

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
                        print(f"not found abnormal system:[{_ref_id}] name:[{name}] in energy.datapoint")

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
                    if _df2.shape[0] <= 0:
                        print(f"meter:[{meter_id}] source_key:[{source_key}] dosnt find right datapoint which has datapoint.id")
                        _energy_datapoint_id = None
                        _ref_id = None
                    else:
                        _ref_id = _df2.iloc[0]['id_x']
                        _energy_datapoint_id = _df2.iloc[0]['id_y']


                # 创建新的NODE, DATAPOINT类型的node
                node_id, node_ref_id = None, None                
                if sys_df.loc[i, "node_ref_id"] =='unknown' and _energy_datapoint_id is not None:                                    
                    data_id = _energy_datapoint_id #sys_df.loc[i, "id_y"]                    

                    try:
                        node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                               data_type="ENERGY", 
                                                               name=sys_df.loc[i, 'name_x'],
                                                            #    desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                            # 如果是多个systems（同一个meterID，相同sourckey），虽然systemID不同，现在统一使用其中一个systemID
                                                            # 这样hierarchy.node_data_points.id 跟 energy.enerngy_datapoint.id就是1对1关系
                                                               desc = f"cid {sys_df.loc[i, 'company_id']} sid {_ref_id}",
                                                               data_id = data_id )         
                    except Exception as e:
                        print(f"create_node DATAPOINT err: {str(e)}")         
                        node_id, node_ref_id = self.hr.query_datapoint_node(data_id)                     
                    
                    
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'       
                    sys_df.loc[i, "node_id"] = node_id       
                    sys_df.loc[i, "node_ref_id"] = node_ref_id  

                # 使用同样meterid, source_key的system，全部更新,
                # 都使用同一个ref_id
                if _ref_id is not None:
                    _df = sys_df[(sys_df['meter_id']==meter_id) & \
                                    (sys_df['source_key']==source_key) & \
                                        (pd.isnull(sys_df['use_system_id']))]
                    
                    for idx in range(len(_df)):
                        row = _df.iloc[idx]                        
                        sys_df.loc[row['index'], "use_system_id"] = _ref_id
                        sys_df.loc[row['index'], "use_datapoint_id"] = _energy_datapoint_id
                        sys_df.loc[row['index'], "ref_id"] = _ref_id
                        sys_df.loc[row['index'], "id_y"] = _energy_datapoint_id

                        sys_df.loc[row['index'], "node_type"] = 'DATAPOINT'       
                        sys_df.loc[row['index'], "node_id"] = node_id       
                        sys_df.loc[row['index'], "node_ref_id"] = node_ref_id  

                
                if not is_none_or_nan(sys_df.loc[i, 'component_of_id']):
                    # 如果是component system,它会在Hierarchy创建一个没有层级关系的datapoint
                    # 为了composition_expression能够使用node_ref_id,必须创建此datapoint
                    sys_df.loc[i, "component"] = 1                     


        # virtual system which has data(kwh) calculated by expression, it also take as data system, 
        # so defined as hierachy.node_datapoint and energy.energy_datapoint                
        pattern = re.compile(r"{id_(\d+)}")
        for i in range(len(sys_df)):
            if not pd.isnull(sys_df.loc[i, 'parent_system_id']) \
                and sys_df.loc[i, 'composition_expression'] is not None \
                and '{' in sys_df.loc[i, 'composition_expression']:
                # 更新composition_expression里边打id_xxx为'node_ref_id'
                try:
                    self.replace_expression_id(sys_df, i)
                except Exception:
                    print('''the expression didn't find any of id_xxx, id_xxx's system maybe are removed, 
                          update the expression to right, otherwise this virtual system will not create in hirachy.node_datapoint, 
                          then you can's see it in the front''')
                    print(f"------A ERR: {sys_df.loc[i, 'composition_expression']} ------")
                    continue
                
                ################################
                ref_id = sys_df.loc[i, "ref_id"]
                energy_datapint_id = sys_df.loc[i, "id_y"]
                tenant_id = sys_df.loc[i, "tenant_id"]
                composition_expression = sys_df.loc[i, "composition_expression"]
                sys_df.loc[i, "expression_replaced"] = 1                

                # create new datapoint
                if np.isnan(ref_id):
                    if force_new:
                        ref_id = sys_df.loc[i, 'id_x']
                        name = sys_df.loc[i, "name_x"]                    
                        meter_id = zeroUUID()
                        new_datapoint_id = self.energy.create_new_datapoint(ref_id, name, meter_id)
                        energy_datapint_id = new_datapoint_id

                        sys_df.loc[i, "use_system_id"] = ref_id
                        sys_df.loc[i, "use_datapoint_id"] = new_datapoint_id
                        sys_df.loc[i, "ref_id"] = ref_id
                        sys_df.loc[i, "id_y"] = new_datapoint_id

                        # 虚拟节点没有sourckey，所以用system name作为它的name
                        self.energy.create_new_virtual_datapoint(tenant_id, new_datapoint_id, name, 
                                                            composition_expression)   
                    else:
                        print(f"not found virtual system:[{sys_df.loc[i, 'id_x']}] in energy.datapoint")                   
                else:
                    # 更新已经存在的virtual_datapoint里边的expression
                    self.energy.update_virtual_datapoint(energy_datapint_id, composition_expression)    

                if sys_df.loc[i, "node_ref_id"] =='unknown':                
                    node_id, node_ref_id = self.hr.create_node(node_type='DATAPOINT', 
                                                               data_type="VIRTUALDATAPOINT", 
                                                               name=sys_df.loc[i, 'name_x'], 
                                                               desc = f"cid {sys_df.loc[i, 'company_id']} sid {sys_df.loc[i, 'id_x']}",
                                                               data_id=energy_datapint_id)                
                    sys_df.loc[i, "node_type"] = 'DATAPOINT'
                    sys_df.loc[i, "node_id"] = node_id
                    sys_df.loc[i, "node_ref_id"] = node_ref_id

        


        # virtual system need refresh the expression again
        # to make sure the expression {id_virtual_datapoint_ref_id_xxx} can be found at this time.
        pattern = re.compile(r"{id_(\d+)}")
        for i in range(len(sys_df)):
            if not np.isnan(sys_df.loc[i, 'parent_system_id']) \
                and sys_df.loc[i, 'composition_expression'] is not None \
                and sys_df.loc[i, "expression_replaced"] == 0:
                # 更新composition_expression里边打id_xxx为'node_ref_id'
                try:
                    self.replace_expression_id(sys_df)
                except Exception:
                    print('''the expression didn't find any of id_xxx, id_xxx's system maybe are removed, please update the expression to right''')                    
                    print(f"------B ERR: {sys_df.loc[i, 'composition_expression']} ------")
                    continue

        # 创建父子节点关系
        # 根据id_x, parent_system_id 关系，在hierarchy 的 relatives里边创建 父子关系        
        print("--------------------------------")
        self.hr.create_relations(sys_df, components_binding = self.components_binding)

        
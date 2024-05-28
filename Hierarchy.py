import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
import xml.etree.ElementTree as ET
from Energy import Energy
import uuid
from common import is_none_or_nan, readOption

class Hierarchy:
    simulation_relations_df = None
    simulation_sys_df = None
    simulation = False
    energy = None

    def __init__(self, host="localhost", port=5432, database="hierarchy", user="hierarchy", password="hierarchy", simulation=False, logger=None):        
        self.logger = logger
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)

        host=readOption("databases.energy.host")
        port=readOption("databases.energy.port")
        database=readOption("databases.energy.database")
        user=readOption("databases.energy.username")
        password=readOption("databases.energy.password")

        self.energy = Energy(host=host, port=port, user=user, password=password, database=database)
        
        self.energy_dp = self.energy.dataPoint()        
        
        self.simulation = simulation        

        
        # Define the list of column names, including the empty column
        columns = ['parent_id', 'parent_type', 'child_id', 'child_type']

        # Create the dataframe with empty columns
        self.simulation_relations_df = pd.DataFrame(columns=columns)

    def nodeDataPointByDataID(self, data_id = None):        
        sql = f'''SELECT id, data_type, ref_id from node_data_points where data_id = '{data_id}' ''' 
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
        
        return dataDF    

    def nodeDataPointIDByDataID(self, data_id = None):        
        sql = f'''SELECT id from node_data_points where data_id = '{data_id}' ''' 
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
        
        return dataDF["id"].iloc[0]

    def nodeDataPoint(self):        
        sql = f'''SELECT id, name, data_type, data_id from node_data_points'''         
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
        
        return dataDF
    
    def nodePov(self):        
        sql = f''' SELECT id, name from node_povs '''        
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
       
        return dataDF
    
    def nodeSite(self):        
        sql = f''' SELECT id, city_id, name FROM node_sites'''        
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
       
        return dataDF
    
    def nodeRelations(self):        
        sql = f''' SELECT id, parent_id, parent_type, child_id, child_type from relations'''        
        dataDF = pd.read_sql_query(sql, self.engine)
        dataDF['id'] = dataDF['id'].astype(str)
       
        return dataDF   
    
    def purgeNodeSite(self, id):
        connection = self.engine.connect()

        # Build your SQL statement as a compiled expression
        sql = text("DELETE FROM node_sites WHERE id = :id")

        # Execute with the connection and provide the parameter value
        connection.execute(sql, id=id)

        connection.close()

    def purgeNodePov(self, id):
        connection = self.engine.connect()

        # Build your SQL statement as a compiled expression
        sql = text("DELETE FROM node_povs WHERE id = :id")

        # Execute with the connection and provide the parameter value
        connection.execute(sql, id=id)

        connection.close()

    def purgeNodeDataPoint(self, id):         
        connection = self.engine.connect()

        # Build your SQL statement as a compiled expression
        sql = text("DELETE FROM node_data_points WHERE id = :id")

        # Execute with the connection and provide the parameter value
        connection.execute(sql, id=id)

        connection.close()

    def purgeRelation(self, parent_id = None, child_id = None):         
        id = child_id   
        sql = text(" delete from relations where child_id = :id")        
        if parent_id:
            id = parent_id
            sql = text(" delete from relations where parent_id = :id")
      
        connection = self.engine.connect()     

        # Execute with the connection and provide the parameter value
        connection.execute(sql, id=id)

        connection.close()

    def purgeTree(self, tenantID, tenantName, tenantCode):
        # purge from child nodes to parent nodes
        # 定义根节点
        root_node = {
            "id": tenantID,            
            "type": "tenant",
            "name": f"{tenantName} | {tenantCode}",
            "children": []
        }

        self._build_or_purge_tree(root_node, purge=True)                
        # purge the root node
        self.purgeRelation(parent_id = f'{tenantID}')
   
    def _build_or_purge_tree(self, node, purge=False):
        sql = f"""
            SELECT
                r.id,
                r.parent_id,
                r.parent_type,
                r.child_id,
                r.child_type
            FROM relations r
            WHERE r.parent_id = '{node["id"]}'
        """
        relations_df = pd.read_sql_query(sql, self.engine)
        if relations_df.shape[0] == 0:
            return

        relations_df['id'] = relations_df['id'].astype(str)
        relations_df['parent_id'] = relations_df['parent_id'].astype(str)
        relations_df['child_id'] = relations_df['child_id'].astype(str)

        children = []
        for i in range(relations_df.shape[0]):
            row = relations_df.iloc[i]
            if row["parent_id"] == node["id"]:                
                child_node = {
                    "id": row["child_id"],
                    "type": row["child_type"],
                    "name": "",
                    "data_id": "",
                    "node_ref_id": "",
                    "system_id": "",
                    "children": []
                }

                # 查询子节点的名称
                if child_node["type"] == "DATAPOINT":                   
                   
                    sql = f"""
                    SELECT name, data_id, ref_id, data_type
                    FROM node_data_points
                    WHERE id = '{child_node["id"]}'
                    """
                    name_df = pd.read_sql_query(sql, self.engine)       
                    if name_df.shape[0] == 0:
                        continue

                    name_df["data_id"] = name_df["data_id"].astype(str)
                    name_df["ref_id"] = name_df["ref_id"].astype(str)

                    child_node["node_ref_id"] = name_df["ref_id"].values[0]
                    child_node["name"] = name_df["name"].values[0]    
                                    
                    data_id = name_df["data_id"].values[0]

                    child_node["data_id"] = data_id

                    system_ref_id = "None"
                    # if data_type is virtual system
                    if name_df["data_type"].values[0] == "VIRTUALDATAPOINT":
                        vdp_df = self.energy.getVirtualDataPointByID(data_id)    
                        if vdp_df.shape[0] == 0:
                            continue
                        child_node["expression"] = vdp_df["expression"].values[0]
                        child_node["type"] = "VIRTUAL_DATAPOINT"

                        data_id = vdp_df["datapoint_id"].values[0]                        
                    
                    try:
                        # system_ref_id = self.energy_dp[self.energy_dp['id'] == data_id]['ref_id'].values[0]
                        dp_df = self.energy.getDataPointByID(data_id)
                        system_ref_id = dp_df["ref_id"].values[0]
                    except:
                        system_ref_id = "xxx"
                    
                    child_node["system_id"] = system_ref_id

                    if purge:
                        self.purgeNodeDataPoint(id = f'{child_node["id"]}')

                elif child_node["type"] == "SITE":
                    sql = f"""
                    SELECT name
                    FROM node_sites
                    WHERE id = '{child_node["id"]}'
                    """
                    name_df = pd.read_sql_query(sql, self.engine)
                    if name_df.shape[0] == 0:
                        continue

                    child_node["name"] = name_df["name"].values[0]

                    if purge:
                        self.purgeNodeSite(id = f'{child_node["id"]}')

                elif child_node["type"] == "POV":
                    sql = f"""
                    SELECT name
                    FROM node_povs
                    WHERE id = '{child_node["id"]}'
                    """
                    name_df = pd.read_sql_query(sql, self.engine)
                    if name_df.shape[0] == 0:
                        continue

                    child_node["name"] = name_df["name"].values[0]
                    
                    if purge:
                        self.purgeNodePov(id = f'{child_node["id"]}')

                # self.logger.debug(child_node["name"])
                children.append(child_node)

                if purge:
                    self.purgeRelation(child_id = f'{child_node["id"]}')

        node["children"] = children
        for child in children:
            self._build_or_purge_tree(child, purge=purge)

    def TenantTree(self, tenantID, tenantName, tenantCode, purge=False):
        # re-get the energy datapoint as some systems migrate
        #  has updated the table's ref_id
        self.energy_dp = self.energy.dataPoint()
        
        # 定义根节点
        root_node = {
            "id": tenantID,            
            "type": "tenant",
            "name": f"{tenantName} | {tenantCode}",
            "children": []
        }

        self._build_or_purge_tree(root_node)

        return root_node
    

    def _build_simulate_tree(self, node):
        str_node_id = f"{node['id']}"
        relations_df = self.simulation_relations_df[self.simulation_relations_df['parent_id']==str_node_id]        

        # relations_df['id'] = relations_df['id'].astype(str)
        relations_df['parent_id'] = relations_df['parent_id'].astype(str)
        relations_df['child_id'] = relations_df['child_id'].astype(str)

        children = []
        for i in range(relations_df.shape[0]):
            row = relations_df.iloc[i]
            child_node = {
                "id": row["child_id"],
                "type": row["child_type"],
                "name": "",
                "data_id": "",
                "node_ref_id": "",
                "system_id": "",
                "children": []
            }

            # 查询子节点的名称
            if child_node["type"] == "DATAPOINT": 
                str_child_node_id = f"{child_node['id']}"
                name_df = self.simulation_sys_df[self.simulation_sys_df['node_id']==str_child_node_id]              

                name_df["id_y"] = name_df["id_y"].astype(str)
                name_df["ref_id"] = name_df["ref_id"].astype(str)
                name_df["node_ref_id"] = name_df["node_ref_id"].astype(str)
                

                child_node["node_ref_id"] = name_df["node_ref_id"].values[0]
                child_node["name"] = name_df["name_x"].values[0]    
                                
                data_id = name_df["id_y"].values[0]

                child_node["data_id"] = data_id
                
                try:
                    system_ref_id = name_df["ref_id"].values[0]
                except:
                    system_ref_id = "None"
                
                child_node["system_id"] = system_ref_id

                # if data_type is virtual system
                if name_df["data_type"].values[0] == "VIRTUALDATAPOINT":
                    child_node["expression"] = name_df["composition_expression"].values[0]
                    child_node["type"] = "VIRTUAL_DATAPOINT"
                

            elif child_node["type"] == "SITE":
                sql = f"""
                SELECT name
                FROM node_sites
                WHERE id = '{child_node["id"]}'
                """
                name_df = pd.read_sql_query(sql, self.engine)
                child_node["name"] = name_df["name"].values[0]

            elif child_node["type"] == "POV":
                str_child_node_id = f"{child_node['id']}"
                name_df = self.simulation_sys_df[self.simulation_sys_df['node_id']==str_child_node_id]
                child_node["name"] = name_df["name_x"].values[0]

            # self.logger.debug(child_node["name"])
            children.append(child_node)            

        node["children"] = children
        for child in children:
            self._build_simulate_tree(child)


    def TenantSimulateTree(self, tenantID, tenantName, tenantCode):
        # 定义根节点
        root_node = {
            "id": tenantID,            
            "type": "tenant",
            "name": f"{tenantName} | {tenantCode}",
            "children": []
        }
        
        self._build_simulate_tree(root_node)

        return root_node
    
    def SaveToXml(self, root, filename):
        """
        将层次结构保存到 XML 文件中。

        Args:
            root: 根节点
            filename: XML 文件名
        """

        # 创建根元素
        root_element = ET.Element(root["type"])
        root_element.attrib["name"] = root["name"]

        # 递归遍历所有节点
        def _recurse(node, parent_element):
            for child in node["children"]:
                child_element = ET.SubElement(parent_element, child["type"])
                child_element.attrib["name"] = child["name"]

                if child["type"] == "DATAPOINT" or child["type"] == "VIRTUAL_DATAPOINT":
                    # 添加其他属性
                    for key, value in child.items():
                        if key not in ("id", "type", "name", "children"):   
                            if str(value) != "":
                                child_element.attrib[key] = str(value)

                _recurse(child, child_element)

        _recurse(root, root_element)

        # 写入 XML 文件
        tree = ET.ElementTree(root_element)
        tree.write(filename)
        
    def create_relations(self, df, components_binding = False):
        '''
        df:
        ['id_x', 'parent_system_id', 'name_x', 'source_key', 'meter_id',
        'composition_expression', 'component_of_id', 'company_id', 'id_y',
        'ref_id', 'name_y', 'tenant_id', 'company', 'company_code', 'index',
        'use_datapoint_id', 'use_system_id', 'node_id', 'node_type',
        'node_ref_id', 'component', 'expression_replaced']

        hierarchy=> select distinct child_type from relations;
            child_type 
            ------------
            POV
            SITE
            DATAPOINT
            (3 rows)

        hierarchy=> select distinct data_type from node_data_points;
            data_type       
            -----------------------
            VIRTUALDATAPOINT
            WATERMETER
            ENERGY
            WATERVIRTUALDATAPOINT
            UNKNOW
            (5 rows)

       '''
        
        # Note: only precess data_type = ENERGY
        # CREATE TYPE node_types AS ENUM
        # ('UNKNOW', 'TENANT', 'LANDLORD', 'SITE', 'ZONE', 'ROOM', 'WORKSTATION', 'POV', 'DATAPOINT');
        NODE_TYPE = ['DATAPOINT', 'POV', 'SITE', 'TENANT']
        NODE_TYPE_LEVEL = {
            "DATAPOINT": 0,            
            "POV": 1,
            "SITE": 2,
            "TENANT": 3,
        }
        len_node_typeLevels = len(NODE_TYPE_LEVEL)
        
        flag = ""
        if self.simulation:
            flag = "Simulation" if self.simulation else ""

        for _, row in df.iterrows():
            child_id = row['node_id']
            child_type = row['node_type']
            # 在导入数据时候，没有找到此system对应的energy.datapoint.id
            # 或则表达式匹配不上，均会导致节点类型为初始值（unknown)
            # 所以不把此类节点建立tree结构，直接忽略
            if child_type == 'unknown':
                continue
            if is_none_or_nan(child_id):
                continue

            if child_type == 'SITE':                
                tenant_id = row['tenant_id']
                sqlInsert = f'''
                        INSERT INTO relations
                        (parent_id, parent_type, child_id, child_type)
                        VALUES ('{tenant_id}', 'TENANT', '{child_id}', '{child_type}')
                        RETURNING id;
                '''
                with self.engine.connect() as connection:                
                    return_id = connection.execute(sqlInsert).fetchone()[0]                        
                    self.logger.debug(f"{flag} Inserted [{return_id}]")

                self.logger.debug(f"{flag} Tenant inserted child_id:[{child_id}]")

            child_type_level = NODE_TYPE_LEVEL[child_type]

            # 默认把tenant_id作为所有节点的父节点
            parent_id = row['tenant_id']

            parent_type_level = child_type_level + 1 if (child_type_level + 1) < len_node_typeLevels else child_type_level                
            parent_type = NODE_TYPE[parent_type_level]
            
            if not components_binding and row['component'] == 1:
                self.logger.debug(f"ignore component child_id in relations:[{child_id}]")
                continue
            
            if not is_none_or_nan(row['parent_system_id']):
                parent_id = row['parent_system_id']
                parent = df[df['id_x']==parent_id]
                if parent.shape[0]==0:
                    continue
                parent_id = parent['node_id'].values[0]
                parent_type = parent['node_type'].values[0]
                if parent_type == 'unknown':
                    continue
                if is_none_or_nan(parent_id):
                    continue

                parent_type_level = NODE_TYPE_LEVEL[parent_type]
                if child_type_level > parent_type_level:
                    error_info = f"child_type:{child_type} child_id:[{child_id}] should under parent_type:{parent_type} parent_id:[{parent_id}]" 
                    # raise ValueError(error_info)
                    self.logger.warning(error_info)
                    continue
                
                # Create a new row as a Series (dictionary-like)
                new_row = pd.Series({                    
                    'parent_id': parent_id,
                    'parent_type': parent_type,
                    'child_id': child_id,
                    'child_type': child_type
                })

                # Append the new row to the dataframe using append for simulate relation dataframe
                self.simulation_relations_df = self.simulation_relations_df.append(new_row, ignore_index=True)
                # self.simulation_relations_df = pd.concat([self.simulation_relations_df, new_row], ignore_index=True)

                # 如果已经有了，不用再创建
                sqlCheck = f"""
                        SELECT id FROM relations
                        where child_id = '{child_id}'
                    """
                dataDF = pd.read_sql_query(sqlCheck, self.engine)
                if dataDF.shape[0] > 0:
                    continue   
                
                sqlInsert = f'''
                        INSERT INTO relations
                        (parent_id, parent_type, child_id, child_type)
                        VALUES ('{parent_id}', '{parent_type}', '{child_id}', '{child_type}')
                        RETURNING id;
                '''
                with self.engine.connect() as connection:                
                    return_id = connection.execute(sqlInsert).fetchone()[0]                        
                    self.logger.debug(f"{flag} Inserted [{return_id}]")

                self.logger.debug(f"{flag} Inserted child_id:[{child_id}]")
                # print(f"{flag} Inserted child_id:[{child_id}]")
            
            

    def query_datapoint_node(self, data_id):
        sql = f''' SELECT id, ref_id FROM node_data_points
                    WHERE data_id = '{data_id}' '''
        
        dataDF = pd.read_sql_query(sql, self.engine)
        if dataDF.shape[0] > 0 and not pd.isnull(dataDF['id'].iloc[0]):
            return dataDF['id'].iloc[0], dataDF['ref_id'].iloc[0]  
        return None, None

    def create_node(self, name, node_type, city_id = None, data_id=None, desc = "", pov_unit="KWH", data_type="ENERGY"):  
        '''
        hierarchy=> select distinct data_type from node_data_points;
            data_type       
        -----------------------
        VIRTUALDATAPOINT
        WATERMETER
        ENERGY
        WATERVIRTUALDATAPOINT
        UNKNOW
        (5 rows)
        '''
        new_id = uuid.uuid4()        

        name = name.replace("'", "''")
        desc = desc.replace("'", "''")      

        mapSqlInsert = {
            "SITE": f"""
                    INSERT INTO node_sites
                    (id, name, city_id, "desc")
                    VALUES ('{new_id}', '{name}', '{city_id}', '{desc}')
                    RETURNING id;
                """,
            "POV": f"""
                    INSERT INTO node_povs
                    (id, name, pov_unit, "desc")
                    VALUES ('{new_id}', '{name}', '{pov_unit}', '{desc}')
                    RETURNING id;
                """,
            "DATAPOINT": f'''
                    INSERT INTO node_data_points
                    (id, name, data_type, data_id, "desc")
                    VALUES ('{new_id}', '{name}',  '{data_type}', '{data_id}', '{desc}' )
                    RETURNING id, ref_id;
                '''            
        }

        mapSqlCheck = {
            "SITE": f"""
                    SELECT id FROM node_sites
                    WHERE "desc" = '{desc}'
                """,
            "POV": f"""                   
                    SELECT id FROM node_povs
                    WHERE "desc" = '{desc}'
                """,
            "DATAPOINT": f'''
                    SELECT id, ref_id FROM node_data_points
                    WHERE "desc" = '{desc}'
                '''            
        }

        insert_sql = mapSqlInsert[node_type]
        # self.logger.debug(f"=====================[{node_type}]===========================")
        # 执行SQL语句并获取新创建数据点的ID
        node_datapoint_id, ref_id = None, None
        if node_type == "DATAPOINT":
            # self.logger.debug(f"---- create node: {node_type}, sql={sql} ----")            
            dataDF = pd.read_sql_query(mapSqlCheck[node_type], self.engine)
            if dataDF.shape[0] > 0 and not pd.isnull(dataDF['id'].iloc[0]):
                # return dataDF['id'].iloc[0], dataDF['ref_id'].iloc[0]    
                # need remove old data as the data_id has changed 
                try:
                    node_datapoint_id = dataDF['id'].iloc[0]
                    self.purgeNodeDataPoint(id = node_datapoint_id)
                except Exception:
                    self.logger.error(f"purge old data failed: data_id:{data_id}") 
                    return False

            toDelete = False
            quit = False
            while not quit:
                with self.engine.connect() as connection:                    
                    try:                        
                        node_datapoint_id, ref_id = connection.execute(insert_sql).fetchone()[:2]
                        quit = True                        
                        if toDelete:
                            self.logger.debug(f"component systme, node type: {node_type} desc: {desc} inserted after remove the old data in node_data_points")
                    except Exception:
                        # 如果插入失败，说明已经有enerngy_datapoin存在了，
                        # 1: 直接提取存对应的node_data_points.id 和 node_data_points.ref_id
                        # 2： 因为node_data_points包含component这种类型的system，而它在关系表中不会出现
                        # 所以导致purge树的时候，不会删除它，只有在这里删除它，并重现创建它
                                                
                        toDelete = True                      

                if toDelete:
                    # 必须node_data_points.id 跟 enerngy_datapoints.id or energy_virtual_datapoint.id 是1 对 1 关系
                    # 否则这个逻辑会有问题
                    try:
                        node_datapoint_id = self.nodeDataPointIDByDataID(data_id=data_id)
                        self.purgeNodeDataPoint(id = node_datapoint_id)
                    except Exception:
                        self.logger.error(f"data_id:{data_id}") 
                        quit = True
                    

            return node_datapoint_id, ref_id
        else:   
            # print(f"---- create node: {node_type}, sql={sql} ----")            
            dataDF = pd.read_sql_query(mapSqlCheck[node_type], self.engine)
            if dataDF.shape[0] > 0 and not pd.isnull(dataDF['id'].iloc[0]):
                # return dataDF['id'].iloc[0]    
                # need remove old data as the data_id has changed 
                try:
                    node_id = dataDF['id'].iloc[0]
                    if node_type == "SITE":
                        self.purgeNodeSite(id = node_id)
                    if node_type == "POV":
                        self.purgeNodePov(id = node_id)
                except Exception:
                    self.logger.error(f"purge old data failed: node_id:{node_id}") 
                                        
            
            with self.engine.connect() as connection:
                try:
                    node_datapoint_id = connection.execute(insert_sql).fetchone()[0]
                except Exception:
                    error_info = f"Error node type: {node_type} desc: {desc} insert failed!!!"
                    raise ValueError(error_info)
            

            return node_datapoint_id
        

    def create_simulate_node(self, name, node_type, city_id = None, data_id=None, desc = "", pov_unit="KWH", data_type="ENERGY", ref_id = 0):          
        '''
        hierarchy=> select distinct data_type from node_data_points;
            data_type       
        -----------------------
        VIRTUALDATAPOINT
        WATERMETER
        ENERGY
        WATERVIRTUALDATAPOINT
        UNKNOW
        (5 rows)
        '''        
        node_datapoint_id = str(uuid.uuid4()) 
        if node_type == "DATAPOINT":                       
            return node_datapoint_id, ref_id
        else:
            return node_datapoint_id
  



import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from Energy import Energy
import uuid
from common import zeroUUID, is_none_or_nan

class Hierarchy:

    def __init__(self, host="localhost", port=5432, database="hierarchy", user="hierarchy", password="hierarchy"):        
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)
        energy = Energy("localhost", "5432", "energy", "energy", "energy")
        self.energy_dp = energy.dataPoint()

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
   
    def _build_tree(self, node):         
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
                    "ref_id": "",
                    "system_id": "",
                    "children": []
                }

                # 查询子节点的名称
                if child_node["type"] == "DATAPOINT":
                    sql = f"""
                    SELECT name, data_id, ref_id
                    FROM node_data_points
                    WHERE id = '{child_node["id"]}'
                    """
                    
                    # print(f"[{node['id']}]: {sql}")

                    name_df = pd.read_sql_query(sql, self.engine)
                    name_df["data_id"] = name_df["data_id"].astype(str)
                    name_df["ref_id"] = name_df["ref_id"].astype(str)

                    child_node["ref_id"] = name_df["ref_id"].values[0]
                    child_node["name"] = name_df["name"].values[0]    
                                    
                    data_id = name_df["data_id"].values[0]

                    child_node["data_id"] = data_id
                    

                    
                    try:
                        system_ref_id = self.energy_dp[self.energy_dp['id'] == data_id]['ref_id'].values[0]
                    except:
                        system_ref_id = "None"
                    
                    child_node["system_id"] = system_ref_id

                elif child_node["type"] == "SITE":
                    sql = f"""
                    SELECT name
                    FROM node_sites
                    WHERE id = '{child_node["id"]}'
                    """
                    name_df = pd.read_sql_query(sql, self.engine)
                    child_node["name"] = name_df["name"].values[0]
                elif child_node["type"] == "POV":
                    sql = f"""
                    SELECT name
                    FROM node_povs
                    WHERE id = '{child_node["id"]}'
                    """
                    name_df = pd.read_sql_query(sql, self.engine)
                    child_node["name"] = name_df["name"].values[0]
                    print(child_node)

                print(child_node["name"])
                children.append(child_node)

        node["children"] = children
        for child in children:
            self._build_tree(child)

    def TenantTree(self, tenantID, tenantName, tenantCode):
        # 定义根节点
        root_node = {
            "id": tenantID,            
            "type": "tenant",
            "name": f"{tenantName} | {tenantCode}",
            "children": []
        }

        self._build_tree(root_node)

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

                if child["type"] == "DATAPOINT":
                    # 添加其他属性
                    for key, value in child.items():
                        if key not in ("id", "type", "name", "children"):                        
                            child_element.attrib[key] = str(value)

                _recurse(child, child_element)

        _recurse(root, root_element)

        # 写入 XML 文件
        tree = ET.ElementTree(root_element)
        tree.write(filename)
        
    def create_relations(self, df):
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

        for _, row in df.iterrows():
            child_id = row['node_id']
            child_type = row['node_type']
            child_type_level = NODE_TYPE_LEVEL[child_type]

            parent_id = row['tenant_id'] #zeroUUID()
            parent_type_level = child_type_level + 1 if (child_type_level + 1) < len_node_typeLevels else child_type_level                
            parent_type = NODE_TYPE[parent_type_level]
            if not is_none_or_nan(row['parent_system_id']):
                parent_id = row['parent_system_id']
                parent = df[df['id_x']==parent_id]
                parent_id = parent['node_id'].values[0]
                parent_type = parent['node_type'].values[0]
                parent_type_level = NODE_TYPE_LEVEL[parent_type]
                if child_type_level > parent_type_level:
                    error_info = f"Error: {child_type} should under the level of {parent_type}"                    
                    raise ValueError(error_info)
            
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
            return_id = self.engine.execute(sqlInsert).fetchone()[0]

            # check insert ok
            print(f"Inserted [{return_id}]")
            


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

        sql = mapSqlInsert[node_type]
        # print(f"=====================[{node_type}]===========================")
        # 执行SQL语句并获取新创建数据点的ID
        if node_type == "DATAPOINT":
            # print(f"---- create node: {node_type}, sql={sql} ----")            
            dataDF = pd.read_sql_query(mapSqlCheck[node_type], self.engine)
            if dataDF.shape[0] > 0 and not pd.isna(dataDF['id'].iloc[0]):
                return dataDF['id'].iloc[0], dataDF['ref_id'].iloc[0]        

            datapoint_id, ref_id = self.engine.execute(sql).fetchone()[:2]

            return datapoint_id, ref_id
        else:
            # print(f"---- create node: {node_type}, sql={sql} ----")            
            dataDF = pd.read_sql_query(mapSqlCheck[node_type], self.engine)
            if dataDF.shape[0] > 0 and not pd.isna(dataDF['id'].iloc[0]):
                return dataDF['id'].iloc[0]            
            
            datapoint_id = self.engine.execute(sql).fetchone()[0]

            return datapoint_id


        


import pandas as pd
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as ET
from collections import defaultdict
from common import is_none_or_nan


class EMS:

    def __init__(self, host="localhost", port=5432, database="mangopie", user="mangopie", password="mangopie"):        
        # Construct the connection string
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Create the engine
        self.engine = create_engine(connection_string)

    def company(self, code="cdnis"):      
        sql = f"select distinct id, name, code from companies_company where code = '{code}' "

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def systems(self, code="cdnis"):        
        # sql = f'''select es.id, es.parent_system_id, es.name, es.source_key, es.meter_id, 
        #             es.composition_expression, es.component_of_id, es.company_id, es.city_id 
        #         from energy_system es 
        #         inner join
        #         (select id 
        #         from companies_company
        #         where code = '{code}'
        #         ) cc
        #         on es.company_id = cc.id'''
        sql = f'''
            select es.id, es.parent_system_id, es.name, es.source_key, es.meter_id, 
                es.composition_expression, es.component_of_id, es.company_id, es.city_id, ec.name as city_name 
            from energy_system es 
            inner join
            (select id 
            from companies_company
            where code = '{code}'
            ) cc
            on es.company_id = cc.id
            left join 
            (select id, name from energy_city) ec 
            on es.city_id = ec.id
        '''

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def virtual_systems(self, code="cdnis"):        
        sql = f'''select es.id, es.name, es.composition_expression, es.component_of_id 
                from energy_system es 
                inner join
                (select id 
                from companies_company
                where code = '{code}'
                ) cc
                on es.company_id = cc.id 
                where es.composition_expression is not null
            '''

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def SystemTree(self, companyID, companyName, companyCode):
        # 定义根节点
        root_node = {
            "id": None, 
            "type": "System",
            "name": f"{companyName} | {companyCode}",
            "companyID": companyID,
            "meter_id": None,
            "source_key": None,
            "children": []
        }        

        self._build_tree(root_node)

        return root_node
    
    def _build_tree(self, node): 
        parent_system_id = f"parent_system_id = {node['id']}"
        if node["id"] is None:
            parent_system_id = "parent_system_id is null"
        
        companyID = node["companyID"]
        sql = f"""
        select id, parent_system_id, component_of_id , name, meter_id, company_id, source_key, composition_expression  
        from energy_system
        where company_id = {companyID} 
        and {parent_system_id}
        """        

        relations_df = pd.read_sql_query(sql, self.engine)        
        

        children = []
        for i in range(relations_df.shape[0]):
            row = relations_df.iloc[i]
            component_of_id = row["component_of_id"]
            nodeType = "System"
            # if component_of_id is not None:
            if not is_none_or_nan(component_of_id):
                nodeType = "Component"    
            else:
                composition_expression = row["composition_expression"]
                if composition_expression is not None \
                    and len(composition_expression) > 0:
                    nodeType = "VirtualSystem"


            child_node = {
                "id": row["id"],
                "name": row["name"],
                "type": nodeType,
                "source_key": row["source_key"],
                "meter_id": row["meter_id"],
                "component_of_id": row["component_of_id"],
                "composition_expression": row["composition_expression"],    
                "companyID": companyID,            
                "children": []
            }

            # logger.debug(child_node["name"])
            children.append(child_node)

        node["children"] = children
        for child in children:
            self._build_tree(child)

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

                # 添加其他属性
                for key, value in child.items():
                    if key not in ("type", "name", "children"):
                        child_element.attrib[key] = str(value)

                _recurse(child, child_element)

        _recurse(root, root_element)

        # 写入 XML 文件
        tree = ET.ElementTree(root_element)
        tree.write(filename)

    
    def meter(self, code="cdnis"):        
        sql = f'''SELECT em.id, status, company_id, failed_counts, report_last_sent, last_online_at, description, has_kva, dataflow_mode, data_type, last_checkonline_at
                FROM energy_meter em 
                inner join
                (select id 
                from companies_company
                where code = '{code}'
                ) cc
                on em.company_id = cc.id 
            '''

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def meterEgauge(self, meter_ptr_id=None):        
        sql = f'''SELECT meter_ptr_id, xml_url
                FROM energy_egaugemeter
                where meter_ptr_id = {meter_ptr_id}
            '''

        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def meterBms(self, meter_ptr_id=None):        
        sql = f'''SELECT meter_ptr_id, "token", username, "password", timezone, "interval", auth_url, gateway_url
                    from energy_bmsmeter 
                    where meter_ptr_id = {meter_ptr_id}
                '''
        # Read the data from the table into the DataFrame
        dataDF = pd.read_sql_query(sql, self.engine)

        return dataDF
    
    def MigrateMeterToEnergy(self, energy=None):
        pass
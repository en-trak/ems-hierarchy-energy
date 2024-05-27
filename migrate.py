import pandas as pd
import numpy as np
import pandas as pd

from EMS import EMS
from Organization import Organization
from Energy import Energy
from Hierarchy import Hierarchy
from Dataflow import DataFlow

import xml.etree.ElementTree as ET
from common import readOption
import os
from pathlib import Path
import logging

def Migrate(code, simulation = True, purgeRelations = False, logger=None):       
    logger.info(f"====================== Migrate Site {code} ==========================")

    simulation = simulation
    purgeRelations = purgeRelations

    # code = readOption("code")
    components_binding = readOption("components_binding")        
    host=readOption("databases.organization.host")
    port=readOption("databases.organization.port")
    database=readOption("databases.organization.database")
    user=readOption("databases.organization.username")
    password=readOption("databases.organization.password")
    org = Organization(host=host, port=port, user=user, password=password, database=database)

    tenant = org.Tenant(code)
    
    logger.info(f"{tenant.name.values[0]} | {tenant.company_code.values[0]} | {tenant.id.values[0]}")

    # purge old data in tenant
    host=readOption("databases.hierarchy.host")
    port=readOption("databases.hierarchy.port")
    database=readOption("databases.hierarchy.database")
    user=readOption("databases.hierarchy.username")
    password=readOption("databases.hierarchy.password")

    hr = Hierarchy(host=host, port=port, user=user, password=password, database=database,logger=logger)
    
    # 
    if not simulation:
        hr.purgeTree(tenant.id.values[0],
                        tenant.name.values[0],
                        tenant.company_code.values[0])  
        
    if purgeRelations:
        logger.info("====================== TenantTree purge ==========================")
        hr.purgeTree(tenant.id.values[0],
                        tenant.name.values[0],
                        tenant.company_code.values[0])  

    
    # generate tenant tree in dataflow
    dataFlow = DataFlow(code, components_binding, simulation=simulation, logger=logger)
    df = dataFlow.PreparingData()
    # df = dataFlow.LoadData()
    logger.info("====================== create_nodes_and_datapoints ==========================")
    dataFlow.create_nodes_and_datapoints(df, tenant.id.values[0])
    # export the df to csv file
    # the node_type is 'unknown' means they will not be in hierarchy tree

    site_path = f"./output/{code}"    
    if simulation:
        srd = dataFlow.hr.simulation_relations_df
        if srd.shape[0] == 0:
            logger.error("Simulation is empty!!! It will no sim_new_{code}.xml generate!!!")            
        else:
            site_node_id = srd[srd['parent_type']=='SITE']['parent_id'].iloc[0]
            
            logger.debug("====================== Simulate TenantTree XML ==========================")    
            tenantTree = dataFlow.hr.TenantSimulateTree(
                                    # tenant.id.values[0],
                                    site_node_id,
                                    tenant.name.values[0],
                                    tenant.company_code.values[0])
            # logger.debug(tenantTree)
            dataFlow.hr.SaveToXml(tenantTree, f"{site_path}/sim_new_{code}.xml") 
    else:        
        srd = dataFlow.hr.simulation_relations_df
        srd.to_csv(f"{site_path}/relations_df_{code}.csv")
        if srd.shape[0] == 0:
            logger.error(f"Simulation is empty!!! It will no new_{code}.xml generate!!!")            
        else:
            site_node_id = srd[srd['parent_type']=='SITE']['parent_id'].iloc[0]
            
            logger.debug("====================== TenantTree XML ==========================")    
            tenantTree = dataFlow.hr.TenantTree(tenant.id.values[0],
                                    tenant.name.values[0],
                                    tenant.company_code.values[0],                                    
                                    purge=False)
            # logger.debug(tenantTree)
            dataFlow.hr.SaveToXml(tenantTree, f"{site_path}/new_{code}.xml")   


    host=readOption("databases.ems.host")
    port=readOption("databases.ems.port")
    database=readOption("databases.ems.database")
    user=readOption("databases.ems.username")
    password=readOption("databases.ems.password")    

    logger.debug("======================= EMS system tree XML =========================")
    ems = EMS(host=host, port=port, user=user, password=password, database=database)
    company = ems.company(code)

    # logger.debug(f"{company.name.values[0]} | {company.code.values[0]} | {company.id.values[0]}")
    companyTree = ems.SystemTree(company.id.values[0],
                                    company.name.values[0],
                                    company.code.values[0])
    
    ems.SaveToXml(companyTree, f"{site_path}/{code}_system.xml")
        # logger.debug(companyTree)

    logger.info(f"====================== Finished {code} ==========================")

def main():
    # Define the logger outside the loop
    logger = logging.getLogger(__name__)
    codes = readOption("codes")
    codes_list = codes.split(",")
    def MigrateSites(codes_list = None):
        for code in codes_list:
            site_path = f"./output/{code}"
            if not Path(f"{site_path}").is_dir(): 
                os.makedirs(site_path)              
    
            # Configure logging specific to this iteration
            logger.handlers = []  # Clear any existing handlers
            logger.setLevel(logging.DEBUG)
            handler = logging.FileHandler(filename=f"{site_path}/{code}.log")
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)

            Migrate(code, simulation = False, purgeRelations = False, logger=logger)

    MigrateSites(codes_list)
    
    
if __name__ == "__main__":
    main()   


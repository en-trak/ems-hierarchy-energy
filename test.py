import pandas as pd
import numpy as np
import pandas as pd

from EMS import EMS
from Organization import Organization
from Energy import Energy
from Hierarchy import Hierarchy
from Dataflow import DataFlow

import xml.etree.ElementTree as ET
from common import zeroUUID, readOption


def main():
    code = readOption("code")
    city_id = zeroUUID()
    
    host=readOption("databases.organization.host")
    port=readOption("databases.organization.port")
    database=readOption("databases.organization.database")
    user=readOption("databases.organization.username")
    password=readOption("databases.organization.password")
    org = Organization(host=host, port=port, user=user, password=password, database=database)

    tenant = org.Tenant(code)
    print(f"{tenant.name.values[0]} | {tenant.company_code.values[0]} | {tenant.id.values[0]}")

    # purge old data in tenant
    host=readOption("databases.hierarchy.host")
    port=readOption("databases.hierarchy.port")
    database=readOption("databases.hierarchy.database")
    user=readOption("databases.hierarchy.username")
    password=readOption("databases.hierarchy.password")

    hr = Hierarchy(host=host, port=port, user=user, password=password, database=database)
    # print("====================== TenantTree purge ==========================")    
    hr.purgeTree(tenant.id.values[0],
                 tenant.name.values[0],
                 tenant.company_code.values[0])  
    
    # generate tenant tree in dataflow
    dataFlow = DataFlow(code)
    df = dataFlow.PreparingData()
    dataFlow.create_nodes_and_datapoints(df, city_id, tenant.id.values[0])
    # export the df to csv file
    # the node_type is 'unknown' means they will not be in hierarchy tree
    df.to_csv(f"./output/new_{code}.csv")
        
    print("====================== TenantTree XML ==========================")    
    tenantTree = hr.TenantTree(tenant.id.values[0],
                               tenant.name.values[0],
                               tenant.company_code.values[0])
    # # print(tenantTree)
    hr.SaveToXml(tenantTree, f"./output/new_{code}.xml")   

    
    # print("======================= EMS system tree XML =========================")
    # ems = EMS(host=host, port=port, user=user, password=password, database=database)
    # company = ems.company(code)

    # # print(f"{company.name.values[0]} | {company.code.values[0]} | {company.id.values[0]}")

    # companyTree = ems.SystemTree(company.id.values[0],
    #                                  company.name.values[0],
    #                                  company.code.values[0])
    
    # ems.SaveToXml(companyTree, f"./output/{code}_system.xml")
    # print(companyTree)
    
    




if __name__ == "__main__":
    main()   
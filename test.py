import pandas as pd
import numpy as np
import pandas as pd

from EMS import EMS
from Organization import Organization
from Energy import Energy
from Hierarchy import Hierarchy
from Dataflow import DataFlow

import xml.etree.ElementTree as ET
from common import zeroUUID


def main():
    code = "cdnis"
    city_id = zeroUUID()
    org = Organization()

    tenant = org.Tenant(code)
    print(f"{tenant.name.values[0]} | {tenant.company_code.values[0]} | {tenant.id.values[0]}")

    # purge old data in tenant
    hr = Hierarchy()
    hr.purgeTree(tenant.id.values[0],
                 tenant.name.values[0],
                 tenant.company_code.values[0])  
    
    # generate tenant tree in dataflow
    # dataFlow = DataFlow(code)
    # df = dataFlow.PreparingData()
    # dataFlow.create_nodes_and_datapoints(df, city_id, tenant.id.values[0])
    
    

    
    
    
    # tenantTree = hr.TenantTree(tenant.id.values[0],
    #                            tenant.name.values[0],
    #                            tenant.company_code.values[0])
    # # # print(tenantTree)
    # hr.SaveToXml(tenantTree, f"./output/{code}.xml")   

    # print("================================================")    
    # ems = EMS()
    # company = ems.company(code)

    # # print(f"{company.name.values[0]} | {company.code.values[0]} | {company.id.values[0]}")

    # companyTree = ems.SystemTree(company.id.values[0],
    #                                  company.name.values[0],
    #                                  company.code.values[0])
    
    # ems.SaveToXml(companyTree, f"./output/{code}_system.xml")
    # print(companyTree)
    
    # print("================================================")




if __name__ == "__main__":
    main()   
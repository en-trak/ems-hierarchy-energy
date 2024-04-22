# ems-hierarchy-energy
https://entrak.atlassian.net/browse/TEP-4560


[TEP-4560]
- generate hierarchy with node_data_points.ref_id
- purge systems of ems using same meterId and sourcekey  
- binding ems systems with existed datapoints of energy
- replace expresion of energy_virtual_datapoint with composition_expression of ems
  using node_data_points.ref_id
- generate xml of energy and ems for testing comparing

# development & deployment
$ ems.ipynb for functionality testing when developing
$ Run python migrate.py for migration

# ./output
there are two sites xml for comparing result

# generate grpc
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. ./energy_virtual_datapoint.proto
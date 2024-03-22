# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# [Unreleased]

## [v1.0.0]

### Added
[TEP-4560]
- generate hierarchy with node_data_points.ref_id
- purge systems of ems using same meterId and sourcekey  
- binding ems systems with existed datapoints of energy
- replace expresion of energy_virtual_datapoint with composition_expression of ems
  using node_data_points.ref_id
- generate xml of energy and ems for testing comparing,
  
Note: the component type systems will showing in the output xml, 
      the parent_system_id is null in database.

# [Released]

---
# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: energy_virtual_datapoint.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1e\x65nergy_virtual_datapoint.proto\x12\x0bsbos.energy\x1a\x1bgoogle/protobuf/empty.proto\x1a\x1egoogle/protobuf/wrappers.proto\"\x92\x02\n\x10VirtualDatapoint\x12\n\n\x02id\x18\x01 \x01(\x0c\x12\x11\n\ttenant_id\x18\x02 \x01(\x0c\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x12\n\nexpression\x18\x04 \x01(\t\x12\x33\n\x06status\x18\x05 \x01(\x0e\x32#.sbos.energy.VirtualDatapointStatus\x12,\n\x08is_solar\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.BoolValue\x12\x14\n\x0c\x64\x61tapoint_id\x18\x07 \x01(\x0c\x12\x0e\n\x06ref_id\x18\x08 \x01(\x03\x12\x34\n\x07\x63omplex\x18\t \x01(\x0e\x32#.sbos.energy.VirtualDatapointStatus\",\n\x1e\x43reateVirtualDatapointResponse\x12\n\n\x02id\x18\x01 \x01(\x0c\"\xb3\x01\n\x1cQueryVirtualDatapointRequest\x12\n\n\x02id\x18\x01 \x01(\x0c\x12\x11\n\ttenant_id\x18\x02 \x01(\x0c\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\x12\n\nexpression\x18\x04 \x01(\t\x12\x33\n\x06status\x18\x05 \x01(\x0e\x32#.sbos.energy.VirtualDatapointStatus\x12\r\n\x05limit\x18\x06 \x01(\x03\x12\x0e\n\x06offset\x18\x07 \x01(\x03\"(\n\x1aGetVirtualDatapointRequest\x12\n\n\x02id\x18\x01 \x01(\x0c\"+\n\x1d\x44\x65leteVirtualDatapointRequest\x12\n\n\x02id\x18\x01 \x01(\x0c\"^\n\x12TestFormulaRequest\x12\x12\n\nexpression\x18\x01 \x01(\t\x12\x34\n\x07\x63omplex\x18\x02 \x01(\x0e\x32#.sbos.energy.VirtualDatapointStatus\"9\n\x13TestFormulaResponse\x12\x12\n\nexpression\x18\x01 \x01(\t\x12\x0e\n\x06result\x18\x02 \x01(\t*J\n\x16VirtualDatapointStatus\x12\x0f\n\x0bVDS_DEFAULT\x10\x00\x12\x0f\n\x0bVDS_DISABLE\x10\x01\x12\x0e\n\nVDS_ENABLE\x10\x02\x32\x83\x04\n\x16\x45nergyVirtualDatapoint\x12T\n\x06\x43reate\x12\x1d.sbos.energy.VirtualDatapoint\x1a+.sbos.energy.CreateVirtualDatapointResponse\x12?\n\x06Update\x12\x1d.sbos.energy.VirtualDatapoint\x1a\x16.google.protobuf.Empty\x12\x63\n\x15QueryVirtualDatapoint\x12).sbos.energy.QueryVirtualDatapointRequest\x1a\x1d.sbos.energy.VirtualDatapoint0\x01\x12M\n\x03Get\x12\'.sbos.energy.GetVirtualDatapointRequest\x1a\x1d.sbos.energy.VirtualDatapoint\x12P\n\x0bTestFormula\x12\x1f.sbos.energy.TestFormulaRequest\x1a .sbos.energy.TestFormulaResponse\x12L\n\x06\x44\x65lete\x12*.sbos.energy.DeleteVirtualDatapointRequest\x1a\x16.google.protobuf.EmptyB)Z\'github.com/en-trak/protobuf/sbos/energyb\x06proto3')

_VIRTUALDATAPOINTSTATUS = DESCRIPTOR.enum_types_by_name['VirtualDatapointStatus']
VirtualDatapointStatus = enum_type_wrapper.EnumTypeWrapper(_VIRTUALDATAPOINTSTATUS)
VDS_DEFAULT = 0
VDS_DISABLE = 1
VDS_ENABLE = 2


_VIRTUALDATAPOINT = DESCRIPTOR.message_types_by_name['VirtualDatapoint']
_CREATEVIRTUALDATAPOINTRESPONSE = DESCRIPTOR.message_types_by_name['CreateVirtualDatapointResponse']
_QUERYVIRTUALDATAPOINTREQUEST = DESCRIPTOR.message_types_by_name['QueryVirtualDatapointRequest']
_GETVIRTUALDATAPOINTREQUEST = DESCRIPTOR.message_types_by_name['GetVirtualDatapointRequest']
_DELETEVIRTUALDATAPOINTREQUEST = DESCRIPTOR.message_types_by_name['DeleteVirtualDatapointRequest']
_TESTFORMULAREQUEST = DESCRIPTOR.message_types_by_name['TestFormulaRequest']
_TESTFORMULARESPONSE = DESCRIPTOR.message_types_by_name['TestFormulaResponse']
VirtualDatapoint = _reflection.GeneratedProtocolMessageType('VirtualDatapoint', (_message.Message,), {
  'DESCRIPTOR' : _VIRTUALDATAPOINT,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.VirtualDatapoint)
  })
_sym_db.RegisterMessage(VirtualDatapoint)

CreateVirtualDatapointResponse = _reflection.GeneratedProtocolMessageType('CreateVirtualDatapointResponse', (_message.Message,), {
  'DESCRIPTOR' : _CREATEVIRTUALDATAPOINTRESPONSE,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.CreateVirtualDatapointResponse)
  })
_sym_db.RegisterMessage(CreateVirtualDatapointResponse)

QueryVirtualDatapointRequest = _reflection.GeneratedProtocolMessageType('QueryVirtualDatapointRequest', (_message.Message,), {
  'DESCRIPTOR' : _QUERYVIRTUALDATAPOINTREQUEST,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.QueryVirtualDatapointRequest)
  })
_sym_db.RegisterMessage(QueryVirtualDatapointRequest)

GetVirtualDatapointRequest = _reflection.GeneratedProtocolMessageType('GetVirtualDatapointRequest', (_message.Message,), {
  'DESCRIPTOR' : _GETVIRTUALDATAPOINTREQUEST,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.GetVirtualDatapointRequest)
  })
_sym_db.RegisterMessage(GetVirtualDatapointRequest)

DeleteVirtualDatapointRequest = _reflection.GeneratedProtocolMessageType('DeleteVirtualDatapointRequest', (_message.Message,), {
  'DESCRIPTOR' : _DELETEVIRTUALDATAPOINTREQUEST,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.DeleteVirtualDatapointRequest)
  })
_sym_db.RegisterMessage(DeleteVirtualDatapointRequest)

TestFormulaRequest = _reflection.GeneratedProtocolMessageType('TestFormulaRequest', (_message.Message,), {
  'DESCRIPTOR' : _TESTFORMULAREQUEST,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.TestFormulaRequest)
  })
_sym_db.RegisterMessage(TestFormulaRequest)

TestFormulaResponse = _reflection.GeneratedProtocolMessageType('TestFormulaResponse', (_message.Message,), {
  'DESCRIPTOR' : _TESTFORMULARESPONSE,
  '__module__' : 'energy_virtual_datapoint_pb2'
  # @@protoc_insertion_point(class_scope:sbos.energy.TestFormulaResponse)
  })
_sym_db.RegisterMessage(TestFormulaResponse)

_ENERGYVIRTUALDATAPOINT = DESCRIPTOR.services_by_name['EnergyVirtualDatapoint']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\'github.com/en-trak/protobuf/sbos/energy'
  _VIRTUALDATAPOINTSTATUS._serialized_start=855
  _VIRTUALDATAPOINTSTATUS._serialized_end=929
  _VIRTUALDATAPOINT._serialized_start=109
  _VIRTUALDATAPOINT._serialized_end=383
  _CREATEVIRTUALDATAPOINTRESPONSE._serialized_start=385
  _CREATEVIRTUALDATAPOINTRESPONSE._serialized_end=429
  _QUERYVIRTUALDATAPOINTREQUEST._serialized_start=432
  _QUERYVIRTUALDATAPOINTREQUEST._serialized_end=611
  _GETVIRTUALDATAPOINTREQUEST._serialized_start=613
  _GETVIRTUALDATAPOINTREQUEST._serialized_end=653
  _DELETEVIRTUALDATAPOINTREQUEST._serialized_start=655
  _DELETEVIRTUALDATAPOINTREQUEST._serialized_end=698
  _TESTFORMULAREQUEST._serialized_start=700
  _TESTFORMULAREQUEST._serialized_end=794
  _TESTFORMULARESPONSE._serialized_start=796
  _TESTFORMULARESPONSE._serialized_end=853
  _ENERGYVIRTUALDATAPOINT._serialized_start=932
  _ENERGYVIRTUALDATAPOINT._serialized_end=1447
# @@protoc_insertion_point(module_scope)

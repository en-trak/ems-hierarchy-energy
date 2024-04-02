import uuid
import numpy as np
import pandas as pd
import yaml
import grpc
import energy_virtual_datapoint_pb2_grpc as vdpGrpc
from google.protobuf.json_format import MessageToDict

STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC = "energy_virtual_datapoint_grpc"

def zeroUUID():
  """
  生成一个全是 0 的 UUID。

  Returns:
    一个全是 0 的 UUID。
  """

  # 创建一个长度为16的bytes对象，每个字节都是0
  zero_bytes = b'\x00' * 16

  # 将bytes对象转换为UUID对象
  zero_uuid = uuid.UUID(bytes=zero_bytes)

  # print(zero_uuid)  # 输出: 00000000-0000-0000-0000-000000000000

  return zero_uuid

def is_none_or_nan(value):
  return value is None or pd.isnull(value)


import yaml

def readOption(options='database.db1.host', config_path='config.yaml'):
  """
  Reads the YAML configuration file and returns the value for the specified option.

  Args:
      options (str, optional): The path to the option within the YAML structure.
          Defaults to 'database.db1.host'.
      config_path (str, optional): Path to the YAML configuration file.
          Defaults to 'config.yaml'.

  Returns:
      str: The retrieved value or None if not found.
  """
  try:
    with open(config_path, 'r') as f:
      config = yaml.safe_load(f)
      option_parts = options.split('.')
      current_level = config
      for part in option_parts:
        if part in current_level:
          current_level = current_level[part]
        else:
          return None
      return current_level
  except FileNotFoundError:
    print(f"Error: YAML configuration file not found at {config_path}")
  except yaml.YAMLError as e:
    print(f"Error: parsing YAML configuration file: {e}")
  return None

# Example usage
option_value = readOption()
if option_value:
  print(f"Option value: {option_value}")
else:
  print("Option not found in the simulated configuration.")



from configparser import ConfigParser

def LoadIniDataBase(filename):
  # # Path to your configuration file
  # filename = "database.ini"

  try:
    # Create a ConfigParser object
    config = ConfigParser()
    
    # Read the configuration file
    config.read(filename)
    
    # Get the database section
    database = config["database"]
    
    # Access configuration values
    host = database["host"]
    port = int(database["port"])  # Convert port to integer
    username = database["username"]
    password = database["password"]
    database_name = database["database_name"]
    
    # Print the configuration details (modify for your use case)
    print(f"Database connection details:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Username: {username}")
    # Avoid printing password for security reasons (use it within your application)
    print(f"  Database Name: {database_name}")

  except FileNotFoundError:
    print(f"Error: File '{filename}' not found.")
  except KeyError as e:
    print(f"Error: Missing key '{e}' in the configuration file.")


def run_grpc(stub, grpcFunction, **kwargs):
  response = None
  try:               
      host = readOption("grpc.energy.host")            
      
      if stub == STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC:
        with grpc.insecure_channel(host) as channel:
          stub = vdpGrpc.EnergyVirtualDatapointStub(channel)
          kwargs["stub"] = stub
          response = grpcFunction(kwargs)
          # response = MessageToDict(response)
          print("virtual datapoint update response: " + response)        
  except grpc.RpcError as e:
      # this is grpc error
      if e.code() == grpc.StatusCode.CANCELLED:
          print("response grpc-error 1:{}".format(str(e)), flush=True)                        
      elif e.code() == grpc.StatusCode.UNAVAILABLE and 'Connection reset by peer' in e.details():
          print("response grpc-error 2:{}".format(str(e)), flush=True)             
      else:
          print("response grpc-error 3:{}".format(str(e)), flush=True)

      return None
  except Exception as e:                    
      print("response e-error:{}".format(str(e)), flush=True)
      return None

  return response
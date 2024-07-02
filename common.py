import uuid
import binascii
import numpy as np
import pandas as pd
import yaml
import grpc
import energy_virtual_datapoint_pb2_grpc as vdpGrpc
from google.protobuf.json_format import MessageToDict
from configparser import ConfigParser
import uuid
import logging
import yaml

def readOption(options='database.db1.host', config_path='config.yaml', logger=None):
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
    logger.error(f"Error: YAML configuration file not found at {config_path}")
  except yaml.YAMLError as e:
    logger.error(f"Error: parsing YAML configuration file: {e}")
  return None


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

  # logger.info(zero_uuid)  # 输出: 00000000-0000-0000-0000-000000000000

  return zero_uuid

def remove_extra_spaces(text):
  """Removes extra spaces from a string, including leading, trailing, and multiple spaces between words.

  Args:
      text: The string to process.

  Returns:
      A new string with extra spaces removed.
  """
  return " ".join(text.split()).strip()

def is_none_or_nan(value):
  return value is None or pd.isnull(value) or value == 'nan' or value == 'None' or value == '-1' 

def is_none_or_nan_zero(value):
  return value is None or pd.isnull(value) or len(str(value)) == 0 \
    or remove_extra_spaces(str(value)) == '' or value == 'nan' or value == 'None' \
    or value == '-1'

def big_endian_uuid(uuid_str):
    # Convert string to UUID object
    uuid_instance = uuid.UUID(uuid_str)

    # Get UUID bytes (native order)
    uuid_bytes = uuid_instance.bytes

    big_endian_bytes = []
    for i in range(16):
        big_endian_bytes.append(uuid_bytes[15 - i])
    
    return bytes(big_endian_bytes)

def small_endian_uuid(uuid_str):
    # Convert string to UUID object
    uuid_instance = uuid.UUID(uuid_str)

    # Get UUID bytes (native order)
    uuid_bytes = uuid_instance.bytes

    big_endian_bytes = []
    for i in range(16):
        big_endian_bytes.append(uuid_bytes[i])
    
    return bytes(big_endian_bytes)

def LoadIniDataBase(filename, logger):
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
    logger.debug(f"Database connection details:")
    logger.debug(f"  Host: {host}")
    logger.debug(f"  Port: {port}")
    logger.debug(f"  Username: {username}")
    # Avoid printing password for security reasons (use it within your application)
    logger.debug(f"  Database Name: {database_name}")

  except FileNotFoundError:
    logger.error(f"File '{filename}' not found.")
  except KeyError as e:
    logger.error(f"Missing key '{e}' in the configuration file.")


def run_grpc(stub, system_id, old_expression, new_expression, grpcFunction, logger, **kwargs):
  response = None
  try:               
      host = readOption("grpc.energy.host")            
      
      if stub == STUB_ENERGY_VIRTUAL_DATAPOINT_GRPC:
        with grpc.insecure_channel(host) as channel:
          stub = vdpGrpc.EnergyVirtualDatapointStub(channel)
          kwargs["stub"] = stub
          response = grpcFunction(kwargs)
          # response = MessageToDict(response)
          if not is_none_or_nan(response):
            logger.info(f"success grpc response: {response}")        
  except grpc.RpcError as e:
      # this is grpc error
      if e.code() == grpc.StatusCode.CANCELLED:
          logger.error("response grpc-error 1:{}".format(str(e)))                        
      elif e.code() == grpc.StatusCode.UNAVAILABLE and 'Connection reset by peer' in e.details():
          logger.error("response grpc-error 2:{}".format(str(e)))             
      else:
          logger.error("SystemID {} \nOLD_Expression:{} \nNEW_Expression:{} \nERROR:{}".format(system_id, old_expression, new_expression, str(e)))

      return None
  except Exception as e:                    
      logger.error("response e-error:{}".format(str(e)))
      return None

  return response



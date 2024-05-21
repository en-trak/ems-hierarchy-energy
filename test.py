from EMS import EMS
from Organization import Organization
from Energy import Energy
from Hierarchy import Hierarchy
from Dataflow import DataFlow
import pandas as pd
import numpy as np

from EMS import EMS
from Organization import Organization
from Energy import Energy
from Hierarchy import Hierarchy
from Dataflow import DataFlow
import xml.etree.ElementTree as ET
from common import readOption
from IPython.display import display
import os
from pathlib import Path
import uuid
import binascii

import logging

# Configure logging to write to a file named "my_app.log"
logging.basicConfig(filename="my_app.log", level=logging.DEBUG)

# Create a logger for your application
logger = logging.getLogger(__name__)

# Log messages at different levels
logger.debug("This is a debug message")
logger.info("This is an informational message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")


if __name__ == "__main__":
    filepath = "./output/isf/sys_df_after.csv"
    # Read the CSV file into a dataframe using read_csv()
    df = pd.read_csv(filepath)
    print(df[:3])
    logger.handlers = []  # Clear any existing handlers
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename=f"test.log")
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    hr = Hierarchy(logger=logger)
    hr.simulation = True
    hr.create_relations(df)

    hr.simulation_relations_df.to_csv("./isf_simulation_relations_df.csv")

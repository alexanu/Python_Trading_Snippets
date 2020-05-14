from enum import Enum
import os
import sys
import inspect
import json
import logging
from pathlib import Path

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)


class Actions(Enum):
    BUY = 1
    SELL = 2
    DEPOSIT = 3
    DIVIDEND = 4
    WITHDRAW = 5
    FEE = 6


class Messages(Enum):
    INSUF_FUNDING = "ERROR: Insufficient funding available"
    INSUF_HOLDINGS = "ERROR: Insufficient holdings available"
    INVALID_OPERATION = "ERROR: Invalid operation"
    ABOUT_MESSAGE = (
        "Creator: Alberto Cardellini\nSource: https://github.com/ilcardella/TradingMate"
    )
    ERROR_SAVE_FILE = "Error saving the log. Try again."
    ERROR_OPEN_FILE = "Error opening the file. Try again."
    UNSAVED_CHANGES = "There are unsaved changes, are you sure?"
    ERROR_SAVE_SETTINGS = "Unable to save the settings"
    WINDOW_UNSUPPORTED_ACTION = "This window does not support the selected action"


class Markets(Enum):
    LSE = "LON"


class Utils:
    """
    Class that provides utility functions
    """

    def __init__(self):
        pass

    @staticmethod
    def load_json_file(filepath):
        """
        Load a JSON formatted file from the given filepath

            - **filepath** The filepath including filename and extension
            - Return a dictionary of the loaded json
        """
        try:
            with open(filepath, "r") as file:
                return json.load(file)
        except Exception as e:
            logging.error("Unable to load JSON file {}".format(e))
        return None

    @staticmethod
    def write_json_file(filepath, data):
        """
        Write a python dict object into a file with json formatting

            -**filepath** The filepath
            -**data** The python dict to write
            - Return True if succed, False otherwise
        """
        try:
            with open(filepath, "w") as file:
                json.dump(data, file, indent=4, separators=(",", ": "))
                return True
        except Exception as e:
            logging.error("Unable to write JSON file: ".format(e))
        return False

    @staticmethod
    def get_install_path():
        """
        Returns the installation path of TradingMate
        """
        return os.path.join(os.sep, "opt", "TradingMate")

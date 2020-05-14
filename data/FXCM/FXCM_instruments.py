import fxcmpy
from fxcmpy import fxcmpy_tick_data_reader as tdr
from fxcmpy import fxcmpy_candles_data_reader as cdr



# https://github.com/yhilpisch/tpqoa/blob/master/tpqoa/tpqoa.py

from pathlib import Path
from configparser import ConfigParser


config = ConfigParser()
config.read(os.path.join(Path(__file__).parents[2], "connections.cfg"))


TOKEN = "5b753c2100d507e6eb0cad29529f1eaf6610e41c"
con = fxcmpy.fxcmpy(config_file='fxcm.cfg', server='demo')
con = fxcmpy.fxcmpy(access_token=TOKEN, log_level='error')
con = fxcmpy.fxcmpy(access_token=config['FXCM']['access_token'], log_file=config['FXCM']['log_file'])

available_symbols_candles_fxcm = cdr.get_available_symbols()
available_symbols_ticks_fxcm = tdr.get_available_symbols()

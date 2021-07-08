import json
import pandas as pd

import os
from configparser import ConfigParser


import fxcmpy
import socketio # the issue was that only version 4.6.1 was ok
# fxcmpy.__version__
# socketio.__version__

token ='ef15106b77ff8333d3e5a0842695f7113538ef98'
con = fxcmpy.fxcmpy(access_token = token, log_level = 'error')
# con = fxcmpy.fxcmpy(access_token=token, log_level='error', server='demo', log_file='log.txt')
instruments = con.get_instruments()
hist_data = pd.DataFrame(instruments).to_csv("FXCM_instr.csv")


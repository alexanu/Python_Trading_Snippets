import os

XTB_USER = os.environ.get('XTB_USER') or 0
XTB_PASS = os.environ.get('XTB_PASS') or '<xtb_password_required>'

from local import *
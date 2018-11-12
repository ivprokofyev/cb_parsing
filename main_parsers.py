#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
pathMyParsers = os.getcwd()+'/parsers'
sys.path.append(pathMyParsers)

from parsers import *
from convdb import *


add_count = 0
add_count += aerodar()
add_count += centravia()

if add_count > 0:
    conv_db_auto()


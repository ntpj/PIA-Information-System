# system
import sys, os
import subprocess, multiprocessing, concurrent, threading
import traceback
import inspect
import random
import time
import gc
from datetime import date, timedelta, datetime
from functools import partial

# key libs
import pandas as pd
import numpy as np
import pickle
import math as m

# others
import copy
import shutil
import glob
try:
    import tkinter as tk
    from tkinter import filedialog
except (ModuleNotFoundError):
    pass
import configparser
import binascii
from wakepy import keep
from difflib import *
from IPython.display import clear_output
import uuid
import timeit
import re
import pathlib

# scrape/info gathering lib
import investpy
import countrynames
import ccy 
from selenium import webdriver 
from bs4 import BeautifulSoup
import requests
import json
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
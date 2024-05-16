from sys_functions import *
import pandas as pd
import numpy as np
import os
from datetime import datetime
import pickle

class cache:
    def __init__(self, name=None, data=None, force=False, path=None):
        self.data = data
        if path is None:
            self.path = f'./cache/{name}.cache'
        else:
            self.path = path
        self.created_date = datetime.now()
        self.last_edited = datetime.now()
        if force:
            self.write(data)
        else:
            self.check_existing(data)
        
    def action(self, act, data):
        with open(self.path, act) as file:
            if act == 'wb':
                pickle.dump(data, file)
            elif act == 'rb':
                self.data = pickle.load(file)
        file.close()
        
    def read(self):
        self.action('rb', None)
        
    def write(self, data):
        self.last_edited = datetime.now()
        self.action('wb', data)
        
    def check_existing(self, data):
        if os.path.exists(self.path):
            self.read()
        else:
            self.write(data)
            
    def cached(self):
        self.write(self.data)


class DataFrameFile:
    def __init__(self, name, dataframe=pd.DataFrame()):
        self.name = name
        self.path = f'./output/{name}.csv'
        self.path_alt = f'./output/{name}.xlsx'
        self.dataframe = dataframe
        self.length = len(dataframe)
        self.set_1 = None
        self.set_2 = None
        self.set_3 = None
        self.dataframe_sorted_inst1 = None
        self.dataframe_sorted_inst2 = None
        self.dataframe_sorted_inst3 = None
        
    def sort(self, sort_by_var, inst=0, convert=False, dropna=False, drop_subset=[], drop_how='all'):
        mapper = {
            "0" : self.dataframe,
            "1" : self.dataframe_sorted_inst1,
            "2" : self.dataframe_sorted_inst2,
            "3" : self.dataframe_sorted_inst3
        }
        if dropna:
            mapper[str(inst)] = self.dataframe.dropna(subset=drop_subset, how=drop_how).sort_values(by=sort_by_var)
            mapper[str(inst)].reset_index(drop=True, inplace=True)
        else:
            mapper[str(inst)] = self.dataframe.sort_values(by=sort_by_var)
            mapper[str(inst)].reset_index(drop=True, inplace=True)
        
        if convert:
            mapper[str(inst)] = convert_to(df=mapper[str(inst)], Type=convert)
            
        setattr(self, f"dataframe_sorted_inst{inst}", mapper[str(inst)])

    def export(self, formt='csv'):
        if formt.lower() == 'csv':
            self.dataframe.to_csv(self.path, index=False)
        elif formt.lower() in ['excel', 'xlsx']:
            self.dataframe.to_excel(self.path_alt, index=False)
        else:
            self.dataframe.to_csv(self.path, index=False)
            
    def length(self):
        self.length = len(self.dataframe)
        
    def update_set(self, keyword, id=1, inst=1):
        mapper_inst = {
            "0" : self.dataframe,
            "1" : self.dataframe_sorted_inst1,
            "2" : self.dataframe_sorted_inst2,
            "3" : self.dataframe_sorted_inst3
        }
        mapper = {
            "1" : self.set_1,
            "2" : self.set_2,
            "3" : self.set_3
        }
        mapper[str(id)] = set(mapper_inst[str(inst)][keyword].tolist())
        
        # update the instance variable
        setattr(self, f"set_{id}", mapper[str(id)])
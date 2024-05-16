from imports import *
from sys_functions import *
from sys_class import *

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def dict_load():
    # Dict list prep
    end_investor_tp = ['324001', '324002', '324003', '324004', '324005', '324007', '324013', '324012']
    pvr_rspn = ['703600001', '703600002', '703600003', '703600004', '703600005']
    
    # end_ivstr_unq_tp dict
    ei_tp_dict_ISIC = { 
        '324001' : '752999', 
        '324002' : '752999', 
        '324003' : '752999', 
        # 324003 Name starts with 0, lookup from BR
        '324013' : '752253', 
        # 324013 Name is not 07033300003, use 752287
        '324006' : '752444', 
        # 324006 Name doesn't have "ประกันสังคม", use 752287
        '324012' : '752999' 
    }
    
    ei_tp_dict_SECTOR = {
        '324001' : '443078', 
        '324002' : '443078', 
        '324003' : '443078', 
        # 324003 Name starts with 0, lookup from BR
        '324013' : '417300005', 
        '324006' : '443054', 
        # 324006 Name doesn't have "ประกันสังคม", use 417300005
        '324012' : '443078'
    }
    
    
    # pvr_rspn dict
    pvr_rspn_dict_ISIC = {
        '703600004' : '752999',
        '703600005' : '752999'
        }
    
    pvr_rspn_dict_SECTOR = {
        '703600004' : '443078',
        '703600005' : '443078'
        }
    
    return end_investor_tp, pvr_rspn, ei_tp_dict_ISIC, ei_tp_dict_SECTOR, pvr_rspn_dict_ISIC, pvr_rspn_dict_SECTOR


def map_attempt(PIA, BR, indx, ref_var): # map with ivstr tp / pvr_role_rspn
    
    ivstr_ID = str(PIA.dataframe.iloc[indx]["RID"])
    ivstr_tpID = str(PIA.dataframe.iloc[indx]["end_ivstr_unq_id_tp"])
    ivstr_nm = str(PIA.dataframe.iloc[indx]["end_ivstr_nm"])
    ivstr_role = str(PIA.dataframe.iloc[indx]["pvr_role_rspn"])
    ivstr_ipid = str(PIA.dataframe.iloc[indx]["org_ip_id"])
    
    new_row = pd.DataFrame({
            'RID': [ivstr_ID],
            'end_ivstr_unq_id_tp' : [ivstr_tpID],
            'end_ivstr_nm' : [ivstr_nm],
            'pvr_role_rspn' : [ivstr_role],
            'org_ip_id' : [ivstr_ipid],
            'SECTOR': [np.NaN],
            'ISIC4_CLID': [np.NaN]
            })

    # load dict (refer to dict_load function above)
    end_investor_tp, pvr_rspn, ei_tp_dict_ISIC, ei_tp_dict_SECTOR, pvr_rspn_dict_ISIC, pvr_rspn_dict_SECTOR = dict_load()

    # define search_dict
    if ref_var == 'end_ivstr_unq_id_tp':
        search_dic = end_investor_tp
    elif ref_var == 'pvr_role_rspn':
        search_dic = pvr_rspn

    # if in search_dict, attempt to map
    if PIA.dataframe.iloc[indx][ref_var] in search_dic:

        # then calls to either of the mapping function above
        if ref_var == 'end_ivstr_unq_id_tp':
            if ( # if 324003 and id starts with 0, dont process
                (
                    str(ivstr_tpID) == "324003"
                    and str(ivstr_ID).startswith("0")
                ) 
                or str(ivstr_tpID) in ["324004", "324005", "324007"] 
            ):
                # search from BR using RID directly  
                new_row_candidate = search_sorted(df=BR.dataframe_sorted_inst2,
                                                    lst=BR.dataframe_sorted_inst2['RID'].astype(str),
                                                    value=str(new_row.iloc[0]['RID']))
                if len(new_row_candidate) > 0:
                    for col in ['RID', 'end_ivstr_nm', 'ISIC4_CLID', 'SECTOR']:
                        if col == 'end_ivstr_nm' and (ivstr_nm != 'nan' and not pd.isna(ivstr_nm)):
                            continue 
                        if col == 'RID' and (ivstr_ID != 'nan' and not pd.isna(ivstr_ID)):
                            continue
                        new_row.loc[0, col] = new_row_candidate.iloc[0][col]


            else:
                # locate ISIC and SECTOR based on type
                new_row.loc[0, "ISIC4_CLID"] = ei_tp_dict_ISIC[ivstr_tpID]
                new_row.loc[0, "SECTOR"] = ei_tp_dict_SECTOR[ivstr_tpID]

                # 324013 Name is not 07033300003, use 752287
                if (
                    str(ivstr_tpID) == "324013" and
                    (str(ivstr_nm) not in ["7033300003", "07033300003"])
                ):
                    new_row.loc[0, "ISIC4_CLID"] = 752287

                # 324006 Name doesn't have "ประกันสังคม", use 752287
                if (str(ivstr_tpID) == "324006" and
                    ("ประกันสังคม" not in str(ivstr_nm))):
                    new_row.loc[0, "ISIC4_CLID"] = 752287
                    new_row.loc[0, "SECTOR"] = 417300005

        elif ref_var == 'pvr_role_rspn':
            if str(ivstr_role) in ['703600001','703600002','703600003']:
                new_row_candidate = search_sorted(df=BR.dataframe_sorted_inst1,
                                                  lst=BR.dataframe_sorted_inst1['IP_ID_BR'].astype(str),
                                                    value=str(ivstr_ipid))
                if len(new_row_candidate) > 0:
                    for col in ['ISIC4_CLID', 'SECTOR']:
                        new_row.loc[0, col] = new_row_candidate.iloc[0][col]
                    new_row.loc[0, "end_ivstr_nm"] = new_row_candidate.iloc[0]['end_ivstr_nm']
                    new_row.loc[0, "RID"] = new_row_candidate.iloc[0]['RID']
                    
            else: # 04 and 05                            
                if pd.isna(new_row.loc[0,"ISIC4_CLID"]):
                    new_row.loc[0, "ISIC4_CLID"] = pvr_rspn_dict_ISIC[ivstr_role]
                if pd.isna(new_row.loc[0,"SECTOR"]):
                    new_row.loc[0, "SECTOR"] = pvr_rspn_dict_SECTOR[ivstr_role]
                if new_row.loc[0, "end_ivstr_nm"] == 'nan':
                    new_row.loc[0, "end_ivstr_nm"] = 'NA'
                if pd.isna(new_row.loc[0, 'RID']):
                    new_row.loc[0, "RID"] = 'NA'

    return new_row
        

def investor_validation(PIA_RECEIVED, INV_RECEIVED):
    
    # Dataframes/cache prep
    PIA = copy.copy(PIA_RECEIVED)
    INV = INV_RECEIVED
    
    BR = DataFrameFile(name='BR', dataframe=df_read(name='BR_N', path='./source_files/'))
    INV_NOT_FOUND = DataFrameFile(name='INV_NOT_FOUND', dataframe=df_read(name='INV_NOT_FOUND', clone=pd.DataFrame(columns=INV.dataframe.columns)))
    INV_CACHE = cache(name="INV", data=pd.DataFrame({'current_name' : [None], 'current_index' : [None]}))
    SECTOR_MAP = DataFrameFile(name='SEC_MAP', dataframe=df_read(name='sector', path='./source_files/'))
    SEC_DICT = SECTOR_MAP.dataframe.set_index('Sector_CL_ID')['Sector_CL_Subgroup'].to_dict()
    
    # Main
    try:
        print(f'{os.path.basename(__file__)}: Investor Running {datetime.now()}')
        
        # PIA pre-processing
        PIA.dataframe = PIA.dataframe.rename(columns={'end_ivstr_id': 'RID'})
        PIA.dataframe = PIA.dataframe.applymap(lambda x: str(x).replace(".0", ""))
        PIA.dataframe = PIA.dataframe.dropna(subset=['RID', 'end_ivstr_nm'], how='all')
        PIA.length = len(PIA_RECEIVED.dataframe)
        PIA.dataframe['RID'] = PIA.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13))
        PIA.dataframe['RID'] = PIA.dataframe['RID'].replace(['nan', '0000000000nan', '000000000None'], np.nan)
        
        # create BR instances
        BR.sort(inst = 1, sort_by_var='IP_ID_BR') # BR instance 1 = BR sorted by IP_ID_BR
        BR.sort(inst = 2, sort_by_var='RID') # BR instance 2 = BR sorted by RID
        BR.dataframe_sorted_inst1['IP_ID_BR'] = BR.dataframe_sorted_inst1['IP_ID_BR'].astype(str).apply(lambda x: str(x).replace(".0", "")).copy()
        BR.dataframe_sorted_inst1['RID'] = BR.dataframe_sorted_inst1['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        BR.dataframe_sorted_inst2['RID'] = BR.dataframe_sorted_inst2['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        
        # investor tables prep
        INV_NOT_FOUND.dataframe = convert_to(INV_NOT_FOUND.dataframe, Type='str')
        INV_NOT_FOUND.sort(inst = 1, sort_by_var='RID')
        INV_NOT_FOUND.sort(inst = 2, sort_by_var='end_ivstr_nm')
        INV.dataframe = convert_to(INV.dataframe, Type='str')
        INV.sort(inst = 1, sort_by_var='RID')
        INV.sort(inst = 2, sort_by_var='end_ivstr_nm')
        INV_og_length = len(INV.dataframe)
        INV.dataframe['RID'] = INV.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).copy()
        INV.dataframe_sorted_inst1['RID'] = INV.dataframe_sorted_inst1['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        INV.dataframe_sorted_inst2['RID'] = INV.dataframe_sorted_inst2['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        INV_NOT_FOUND.dataframe['RID'] = INV_NOT_FOUND.dataframe['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        # INV_NOT_FOUND.dataframe['RID'] = INV_NOT_FOUND.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13))
        # more pre-processing + zfill13 (automatic csv reading will read as int, hence leading 0s are removed, so zfill 13 again)
        # .apply(lambda x: x.zfill(13)).apply(lambda x: str(x).replace(".0", ""))
        INV.dataframe_sorted_inst2['org_ip_id'] = INV.dataframe_sorted_inst2['org_ip_id'].astype(str).apply(lambda x: str(x).replace(".0", "")).copy()
        INV_NOT_FOUND.dataframe_sorted_inst2['org_ip_id'] = INV_NOT_FOUND.dataframe_sorted_inst2['org_ip_id'].astype(str).apply(lambda x: str(x).replace(".0", "")).copy()

        
        # Cache prep
        if INV_CACHE.data.iloc[0]['current_name'] is None:
            INV_CACHE.data.loc[0, 'current_name'] = PIA.name
            INV_CACHE.data.loc[0, 'current_index'] = 0
        else:
            pass
        
        
        # generate set
        INV.update_set(id=1, keyword='RID', inst=1)
        INV.update_set(id=2, keyword='end_ivstr_nm', inst=2)
        INV.update_set(id=3, keyword='org_ip_id', inst=2)
        INV_NOT_FOUND.update_set(id=1, keyword='RID', inst=1)
        INV_NOT_FOUND.update_set(id=2, keyword='end_ivstr_nm', inst=2)
        INV_NOT_FOUND.update_set(id=3, keyword='org_ip_id', inst=2)

        ### actual end investor process
        print(f'{os.path.basename(__file__)}: Starting for-loop from {INV_CACHE.data.loc[0, "current_index"]} {datetime.now()}')
        for row in range(INV_CACHE.data.iloc[0]['current_index'], PIA.length):            
            
            if INV_CACHE.data.iloc[0]['current_index'] == -1 or INV_CACHE.data.iloc[0]['current_index'] >= PIA.length:
                break
            
            # If RID is found in either INV or INV_NF
            elif not pd.isna(PIA.dataframe.loc[row, "RID"]) and (PIA.dataframe.iloc[row]["RID"] in INV.set_1 or PIA.dataframe.iloc[row]["RID"] in INV_NOT_FOUND.set_1):
                pass
            
            # If name is found in either INV or INV_NF
            elif PIA.dataframe.iloc[row]["end_ivstr_nm"].lower() not in ['nan', 'null'] and (PIA.dataframe.iloc[row]["end_ivstr_nm"] in INV.set_2 or PIA.dataframe.iloc[row]["end_ivstr_nm"] in INV_NOT_FOUND.set_2):
                # print(f'found {row}: {PIA.dataframe.iloc[row]["RID"]} or {PIA.dataframe.iloc[row]["end_ivstr_nm"]}') # debug
                pass
            
            # If org_ip_id (and name+rid is nan) is found in either INV or INV_NF
            elif (PIA.dataframe.iloc[row]["end_ivstr_nm"].lower() in ['nan', 'null'] and pd.isna(PIA.dataframe.iloc[row]["RID"])) and (str(PIA.dataframe.iloc[row]['org_ip_id']) in INV.set_3 or str(PIA.dataframe.iloc[row]['org_ip_id']) in INV_NOT_FOUND.set_3):
                pass
            
            # else go with dict condition to map, then BR last
            else:
                new_row = pd.DataFrame({
                        'RID': [PIA.dataframe.iloc[row]['RID']],
                        'end_ivstr_unq_id_tp' : [PIA.dataframe.iloc[row]['end_ivstr_unq_id_tp']],
                        'end_ivstr_nm' : [PIA.dataframe.iloc[row]['end_ivstr_nm']],
                        'pvr_role_rspn' : [PIA.dataframe.iloc[row]['pvr_role_rspn']],
                        'org_ip_id' : [PIA.dataframe.iloc[row]['org_ip_id']],
                        'SECTOR': [np.NaN],
                        'ISIC4_CLID': [np.NaN]
                        })
                        
                # map with RID first        
                if not pd.isna(new_row.loc[0, 'RID']) and new_row.loc[0]['RID'] != 'nan':
                    new_row_candidate = search_sorted(df=BR.dataframe_sorted_inst2,
                                                    lst=BR.dataframe_sorted_inst2['RID'].astype(str),
                                                        value=str(new_row.iloc[0]['RID']))
                    if len(new_row_candidate) > 0:
                        for col in ['ISIC4_CLID', 'SECTOR', 'end_ivstr_nm']:
                            if col == 'end_ivstr_nm' and (not pd.isna(new_row.loc[0, 'end_ivstr_nm']) and str(new_row.iloc[0]['end_ivstr_nm']) != 'nan'):
                                continue
                            new_row.loc[0, col] = new_row_candidate.iloc[0][col]
                
                
                # if still empty, use pvr_role_rspn / end_ivstr_unq_id_tp
                if pd.isna(new_row.loc[0, 'ISIC4_CLID']) or pd.isna(new_row.loc[0, 'SECTOR']):
                    for tp in ['end_ivstr_unq_id_tp', 'pvr_role_rspn']:
                        if PIA.dataframe.iloc[row][tp] is None:
                            PIA.dataframe.loc[row, tp] = np.NaN
                    if pd.isna(new_row.loc[0, 'end_ivstr_unq_id_tp']) or new_row.iloc[0,]['end_ivstr_unq_id_tp'] == 'nan': # if tp empty, use pvr_role
                        new_row = map_attempt(PIA=PIA, BR=BR, indx = row, ref_var = 'pvr_role_rspn')
                    else:
                        new_row = map_attempt(PIA=PIA, BR=BR, indx = row, ref_var = 'end_ivstr_unq_id_tp')
                
                
                ###### post locating process
                new_row['RID'] = new_row['RID'].apply(lambda x: str(x).zfill(13))
                new_row['org_ip_id'] = new_row['org_ip_id'].apply(lambda x: str(x))
                
                mapper_set = {
                    'RID' : 1,
                    'end_ivstr_nm' : 2
                }
                
                # both ISIC and SECTOR are still empty
                if pd.isna(new_row.loc[0,'ISIC4_CLID']) and pd.isna(new_row.loc[0,'SECTOR']) or (int(new_row.iloc[0]['SECTOR']) not in SEC_DICT): #or (new_row.iloc[0]['SECTOR'] not in SEC_DICT):
                    new_row[['ISIC4_CLID', 'SECTOR']] = new_row[['ISIC4_CLID', 'SECTOR']].applymap(lambda x: str(x).replace(".0", ""))
                    
                    NF_mapper_df = { # .apply(lambda x: x.zfill(13)) # .apply(lambda x: str(x).replace(".0", ""))
                        'RID' : INV_NOT_FOUND.dataframe_sorted_inst1,
                        'end_ivstr_nm' : INV_NOT_FOUND.dataframe_sorted_inst2
                    }
                    
                    # update for each instance
                    for key in ['RID', 'end_ivstr_nm']:
                        # row_ind = NF_mapper_df[key][key].astype(str).searchsorted(str(new_row.iloc[0][key]))
                        # NF_mapper_df[key] = pd.concat([NF_mapper_df[key].iloc[:row_ind], new_row, NF_mapper_df[key].iloc[row_ind:]], ignore_index=True)
                        NF_mapper_df[key] = pd.concat([NF_mapper_df[key], new_row], ignore_index=True)
                        NF_mapper_df[key].reset_index(drop=True, inplace=True)
                        NF_mapper_df[key] = convert_to(NF_mapper_df[key], Type='str')
                        if key == 'RID':
                            INV_NOT_FOUND.dataframe_sorted_inst1 = NF_mapper_df[key]
                            INV_NOT_FOUND.update_set(id=mapper_set[key], keyword='RID', inst=mapper_set[key])
                        elif key == 'end_ivstr_nm':
                            INV_NOT_FOUND.dataframe_sorted_inst2 = NF_mapper_df[key]
                            INV_NOT_FOUND.update_set(id=mapper_set[key], keyword='end_ivstr_nm', inst=mapper_set[key])
                            INV_NOT_FOUND.update_set(id=3, keyword='org_ip_id', inst=2)
                        
                # join new_row to INV
                else:
                    new_row.loc[0, 'SECTOR'] = str(SEC_DICT[int(new_row.iloc[0]['SECTOR'])])
                    F_mapper_df = {
                        'RID' : INV.dataframe_sorted_inst1,
                        'end_ivstr_nm' : INV.dataframe_sorted_inst2,
                    }   
                    
                    for key in ['RID', 'end_ivstr_nm']:
                        # row_ind = F_mapper_df[key][key].astype(str).searchsorted(str(new_row.iloc[0][key]))
                        # F_mapper_df[key] = pd.concat([F_mapper_df[key].iloc[:row_ind], new_row, F_mapper_df[key].iloc[row_ind:]], ignore_index=True)
                        F_mapper_df[key] = pd.concat([F_mapper_df[key], new_row], ignore_index=True)
                        F_mapper_df[key].reset_index(drop=True, inplace=True)
                        F_mapper_df[key] = convert_to(F_mapper_df[key], Type='str')
                        if key == 'RID':
                            INV.dataframe_sorted_inst1 = F_mapper_df[key]
                            INV.update_set(id=mapper_set[key], keyword='RID', inst=mapper_set[key])
                        elif key == 'end_ivstr_nm':
                            INV.dataframe_sorted_inst2 = F_mapper_df[key]
                            INV.update_set(id=mapper_set[key], keyword='end_ivstr_nm', inst=mapper_set[key])
                            INV.update_set(id=3, keyword='org_ip_id', inst=2)
            
            # auto export every 10% processed
            try:
                if row % (m.floor(PIA.length*0.1)) == 0 and row != 0:
                    print(f'{os.path.basename(__file__)}: Automatic exported {datetime.now()}, {row} checked')
                    INV_CACHE.data.loc[0, 'current_index'] = row-1
                    INV_CACHE.cached()
                    INV.dataframe = INV.dataframe_sorted_inst1
                    INV.dataframe['RID'] = INV.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
                    INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe_sorted_inst1
                    INV_NOT_FOUND.dataframe['RID'] = INV_NOT_FOUND.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
                    INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
                    INV.dataframe = INV.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
                    INV.export()
                    INV_NOT_FOUND.export(formt='excel')
            except (ZeroDivisionError):
                pass

    except (KeyboardInterrupt):
        INV_CACHE.data.loc[0, 'current_index'] = row-1
        INV_CACHE.cached()
        INV.dataframe = INV.dataframe_sorted_inst1
        INV.dataframe['RID'] = INV.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe_sorted_inst1
        INV_NOT_FOUND.dataframe['RID'] = INV_NOT_FOUND.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV.dataframe = INV.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV.export()
        INV_NOT_FOUND.export(format = 'excel')
        
    except Exception as e:
        traceback_log()
        raise e
        
    else:
        finished(os.path.basename(__file__))
        if INV_CACHE.data.loc[0, 'current_index'] == -1:
            print(f'* {os.path.basename(__file__)}: No changes were made to INV (Investor part already processed)')
        elif len(INV.dataframe) == INV_og_length:
            print(f'* {os.path.basename(__file__)}: No changes were made to INV (All investors can be located from INVESTOR TABLE)')
        elif len(INV.dataframe) != INV_og_length:
            print(f'* {os.path.basename(__file__)}: Changes were made to INV')
        
        if len(INV_NOT_FOUND.dataframe) > 0:
            print(f'* {os.path.basename(__file__)}: Missing data are found in INV_NOT_FOUND file')

        INV_CACHE.data.loc[0, 'current_index'] = -1
        INV.dataframe = INV.dataframe_sorted_inst1
        INV.dataframe['RID'] = INV.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe_sorted_inst1
        INV_NOT_FOUND.dataframe['RID'] = INV_NOT_FOUND.dataframe['RID'].astype(str).apply(lambda x: x.zfill(13)).replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV_CACHE.data.loc[0, 'current_index'] = row
        INV_NOT_FOUND.dataframe = INV_NOT_FOUND.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV.dataframe = INV.dataframe.replace(['nan', '0000000000nan', '000000000None'], np.nan)
        INV.export()
        INV_NOT_FOUND.export(format = 'excel')
        
    finally:
        return INV
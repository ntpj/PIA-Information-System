from imports import *
from sys_functions import *
from sys_class import *
from sys_investor import map_attempt, dict_load

def get_zfill(x):
    return str(x).zfill(13)

def replace_values(value):
    return str(value).replace(".0", "")

def find_ccy_id(ccy, ccy_lst_str, ccy_id):
    result = search_sorted(df=ccy, lst=ccy_lst_str, value=str(ccy_id))
    if len(result) > 0:
        return str(result.iloc[0]['CCY_ID']).replace(".0", "")
    else:
        return '998999'

def _chunk_process_(chunk, IVSTR, SECTY):
    import pandas as pd
    
    thread_no = threading.get_ident()
    INVESTOR_TABLE = IVSTR
    SECURITY_TABLE = SECTY
    
    INVESTOR_TABLE.dataframe = IVSTR.dataframe.copy()
    SECURITY_TABLE.dataframe = SECTY.dataframe.copy()
    # BR = pd.read_csv("./source_files/BR.csv").sort_values('org_ip_id')
    # BR_set = set(BR['org_ip_id'].astype(str))
    
    processed_chunk = pd.DataFrame(columns=['msr_prd_id', 'end_ivstr_unq_id_tp',
                                            'pvr_role_rspn', 'org_ip_id',
                                            'RID', 'end_ivstr_nm',
                                            'SECTOR', 'ISIC4_CLID',
                                            'orig_ccy_cost_val',
                                            'orig_ccy_mkt_val', 'scr_code',
                                            'BOP_item','issuer_name',
                                            'issuer_countryID','scr_currencyID',
                                            'scr_termID', 'scr_instrument'
                                    ])
    # PIA Cleansing (mapping) Process
    try:
        current_time = datetime.now().strftime("%H:%M")
        
        INV_RID_LST = INVESTOR_TABLE.dataframe_sorted_inst1['RID'].astype(str).apply(get_zfill)
        INV_INM_LST = INVESTOR_TABLE.dataframe_sorted_inst2['end_ivstr_nm'].astype(str)
        INV_IPID_LST = INVESTOR_TABLE.dataframe_sorted_inst3['org_ip_id'].astype(str)
        SEC_SCR_LST = SECURITY_TABLE.dataframe['scr_code'].astype(str)

        DUPED_INV_RID_SET = set(INVESTOR_TABLE.dataframe['RID'][INVESTOR_TABLE.dataframe['RID'].duplicated(keep=False)].tolist())
        DUPED_INV_INM_SET = set(INVESTOR_TABLE.dataframe['end_ivstr_nm'][INVESTOR_TABLE.dataframe['end_ivstr_nm'].duplicated(keep=False)].tolist())
        INV_INM_SET = set(INV_INM_LST)
        INV_RID_SET = set(INV_RID_LST)
        SEC_SCR_SET = set(SEC_SCR_LST)
        ccy = sort_df(df= pd.read_csv("./source_files/ccy.csv"), sort_by='CCY')
        
        chunk_obj = DataFrameFile(name=f'chunk_df_{thread_no}', dataframe=chunk.copy())
        chunk_obj.dataframe = chunk_obj.dataframe.rename(columns={'end_ivstr_id': 'RID'})
        
        ccy_lst_str = ccy['CCY'].astype(str)
        # processed_chunk = []

        for row in range(0, len(chunk)):
            
            # some rows that required more process
            SCR_KEY = str(chunk.iloc[row]['scr_code'])
            RID_KEY = str(chunk.iloc[row]['end_ivstr_id'])
            if pd.isna(RID_KEY) or RID_KEY == 'nan':
                RID_KEY = np.NaN
            else:
                RID_KEY = str(RID_KEY).zfill(13).replace(".0", "")
            INM_KEY = str(chunk.iloc[row]['end_ivstr_nm'])  
            if pd.isna(INM_KEY) or INM_KEY == 'nan':
                INM_KEY = np.NaN
            COST_VAL = chunk.iloc[row]['orig_ccy_cost_val']
            if pd.isna(COST_VAL) or str(COST_VAL).replace(".0", "") == str(0) or round(COST_VAL, 0) == 0:
                COST_VAL = chunk.iloc[row]['orig_ccy_mkt_val']
            
            # *** Security matching
            sec_side = pd.DataFrame({
                    'scr_code' : [np.NaN],
                    'BOP_item': [np.NaN],
                    'issuer_name': [np.NaN],
                    'issuer_countryID': [np.NaN],
                    'scr_currencyID' : [find_ccy_id(ccy, ccy_lst_str, chunk.iloc[row]['ccy_id'])],
                    'scr_termID': [np.NaN],
                    'scr_instrument': [np.NaN]
                    })
            if SCR_KEY.replace(".0", "") in SEC_SCR_SET:
                row_sec = search_sorted(df=SECURITY_TABLE.dataframe,
                                    lst=SEC_SCR_LST,
                                    value=SCR_KEY.replace(".0", "")
                                    )
                if len(row_sec) > 0:
                    sec_side = row_sec.iloc[[0]]
                
                
            # *** Investor matching    
            inv_side = pd.DataFrame({
                    'RID': [RID_KEY],
                    'end_ivstr_unq_id_tp': [chunk.iloc[row]['end_ivstr_unq_id_tp']],
                    'end_ivstr_nm' : [INM_KEY],
                    'pvr_role_rspn': [chunk.iloc[row]['pvr_role_rspn']],
                    'org_ip_id': [chunk.iloc[row]['org_ip_id']],
                    'SECTOR': [np.NaN],
                    'ISIC4_CLID': [np.NaN],
                    })
            
            # if both name and rid is empty
            # print(f'{thread_no} : {RID_KEY} {type(RID_KEY)} / {INM_KEY} {type(INM_KEY)} / {str(chunk.iloc[row]["pvr_role_rspn"]).replace(".0", "")} / {str(chunk.iloc[row]["org_ip_id"]).replace(".0", "")}')

            if pd.isna(chunk.iloc[row]['end_ivstr_unq_id_tp']) or (pd.isna(RID_KEY) or RID_KEY == ['nan', '0000000000nan',  '000000000None']) and (pd.isna(INM_KEY) or INM_KEY == 'nan'):
                # pvr_role_rspn is 01-03, search and update by org_ip_id
                if str(chunk.iloc[row]['pvr_role_rspn']).replace(".0", "") in ['703600001', '703600002', '703600003']:
                    
                    row_inv = search_sorted(df=INVESTOR_TABLE.dataframe_sorted_inst3,
                                        lst=INV_IPID_LST,
                                        value=str(chunk.iloc[row]['org_ip_id']).replace(".0", "")
                                        )
                        
                    if len(row_inv) > 0:
                        inv_side = row_inv.iloc[[0]]
                        for col in ['end_ivstr_unq_id_tp', 'pvr_role_rspn', 'org_ip_id']:    
                            inv_side.loc[0, col] = chunk.iloc[row][col]
                            
                    # elif (pd.isna(inv_side.loc[0, 'SECTOR']) or pd.isna(inv_side.loc[0, 'ISIC4_CLID']))and str(chunk.iloc[row]['org_ip_id']).replace(".0", "") in BR_set:
                        
                # if in 04-05, rid and name is NA for grouping
                elif str(chunk.iloc[row]['pvr_role_rspn']).replace(".0", "") in ['703600004', '703600005']:
                    RID_KEY = 'NA'
                    INM_KEY = 'NA'
                    inv_side.loc[0, 'SECTOR'] = '443078'
                    inv_side.loc[0, 'ISIC4_CLID'] = '752999'
                
            # if in name in found list and name is not empty
            elif (str(INM_KEY) in INV_INM_SET and
                  not pd.isna(INM_KEY) and INM_KEY != 'nan' and
                  str(INM_KEY) not in DUPED_INV_INM_SET):
                row_inv = search_sorted(df=INVESTOR_TABLE.dataframe_sorted_inst2,
                                    lst=INV_INM_LST,
                                    value=str(chunk.iloc[row]['end_ivstr_nm'])
                                    )
                if len(row_inv) > 0:
                    inv_side = row_inv.iloc[[0]]

            # if in RID in found list and RID is not empty
            elif (str(RID_KEY).replace(".0", "").zfill(13) in INV_RID_SET and
                  not pd.isna(RID_KEY) and RID_KEY != 'nan' and
                  str(RID_KEY) not in DUPED_INV_RID_SET):
                row_inv = search_sorted(df=INVESTOR_TABLE.dataframe_sorted_inst1,
                                    lst=INV_RID_LST,
                                    value=str(chunk.iloc[row]['end_ivstr_id']).replace(".0", "").zfill(13)
                                    )
                if len(row_inv) > 0:
                    inv_side = row_inv.iloc[[0]]

            # if name or rid is in a duplicated list, only locate where both RID and NAME matches
            elif (not pd.isna(INM_KEY) and not INM_KEY != 'nan' and 
                  not pd.isna(RID_KEY) and not RID_KEY != 'nan' and
                  (str(INM_KEY) in DUPED_INV_INM_SET or str(RID_KEY) in DUPED_INV_RID_SET)):
                row_inv = INVESTOR_TABLE.dataframe.loc[(INVESTOR_TABLE.dataframe['RID'] == RID_KEY) & 
                                             (INVESTOR_TABLE.dataframe['end_ivstr_nm'] == INM_KEY)].reset_index()
                if len(row_inv) > 0:
                    inv_side = row_inv.iloc[[0]]
                
                if (pd.isna(row_inv.iloc[0]['SECTOR']) or str(row_inv.iloc[0]['SECTOR']).lower() in ['nan', 'NULL'] 
                    or pd.isna(row_inv.iloc[0]['ISIC4_CLID']) or str(row_inv.iloc[0]['ISIC4_CLID']).lower() in ['nan', 'NULL']):
                    BR = pd.read_csv('./source_files/BR_N.csv', index=False)
                    inv_side = map_attempt(chunk, BR, row, 'end_ivstr_unq_id_tp')
                    
                    
            # generate new row ## could use some improvement here but idk how to do it :(
            new_row = pd.DataFrame({'msr_prd_id': chunk.iloc[row]['msr_prd_id'], # main key 1
                    'end_ivstr_unq_id_tp': inv_side.iloc[0]['end_ivstr_unq_id_tp'],
                    'pvr_role_rspn': inv_side.iloc[0]['pvr_role_rspn'],
                    'org_ip_id': inv_side.iloc[0]['org_ip_id'],
                    'RID': inv_side.iloc[0]['RID'], # locator key 1
                    'end_ivstr_nm': inv_side.iloc[0]['end_ivstr_nm'],
                    'SECTOR' : inv_side.iloc[0]['SECTOR'],
                    'ISIC4_CLID' : inv_side.iloc[0]['ISIC4_CLID'],
                    'orig_ccy_cost_val' : COST_VAL, # value key 1
                    'orig_ccy_mkt_val' : chunk.iloc[row]['orig_ccy_mkt_val'], # value key 2
                    'scr_code' : SCR_KEY, # locator key 2
                    'BOP_item' : sec_side.iloc[0]['BOP_item'],
                    'issuer_name' : sec_side.iloc[0]['issuer_name'],
                    'issuer_countryID' : sec_side.iloc[0]['issuer_countryID'],
                    'scr_currencyID' : sec_side.iloc[0]['scr_currencyID'],
                    'scr_termID' : sec_side.iloc[0]['scr_termID'],
                    'scr_instrument' : sec_side.iloc[0]['scr_instrument'],
                    }, index=[0])
            
            # merge back then export
            # new_row = pd.DataFrame(new_row, index=[0])
            # processed_chunk.append(new_row)
            processed_chunk.loc[len(processed_chunk), :] = new_row.loc[0] # YEARLY_PIA_DF.dataframe = pd.concat([YEARLY_PIA_DF.dataframe, new_row])
            # show time output to terminal
            '''            
            try:
                if datetime.now().strftime("%H:%M") != current_time:
                    pass
                    # print(f'[{str(thread_no).zfill(13)}/{current_time}]: Processing at row #{row}')
                current_time = datetime.now().strftime("%H:%M")
                # gc.collect()
            except (NameError):
                current_time = datetime.now().strftime("%H:%M")
            except:
                pass
            '''
        
    except (KeyboardInterrupt):
        raise (KeyboardInterrupt)

    except Exception as e:
        traceback_log()
        raise e
    
    else:
        pass
        # processed_chunk = pd.concat(processed_chunk)
      
    finally:  
        return processed_chunk

if __name__ == "__main__":
    
    start_time = datetime.now()
    
    try:
        print(f'[{os.path.basename(__file__)}/{datetime.now()}]: loading cache')
        PIA_PROCESS_CACHE = cache(name='PIA_PROCESS_CACHE')
        YEARLY_PIA_DF = PIA_PROCESS_CACHE.data[0]
        NEW_PIA_DF =  PIA_PROCESS_CACHE.data[1]
        
        INVESTOR_TABLE = PIA_PROCESS_CACHE.data[2]
        SECURITY_TABLE = PIA_PROCESS_CACHE.data[3]
        
        INVESTOR_TABLE.sort(inst = 1, sort_by_var='RID', dropna=True, drop_subset=['RID'])
        INVESTOR_TABLE.dataframe_sorted_inst1['RID'] = INVESTOR_TABLE.dataframe_sorted_inst1['RID'].astype(str).apply(lambda x: str(x).zfill(13).replace(".0", "")).copy()
        INVESTOR_TABLE.sort(inst = 2, sort_by_var='end_ivstr_nm', dropna=True, drop_subset=['end_ivstr_nm'])
        INVESTOR_TABLE.sort(inst = 3, sort_by_var='org_ip_id', dropna=True, drop_subset=['org_ip_id'])
        INVESTOR_TABLE.dataframe_sorted_inst3['org_ip_id'] = INVESTOR_TABLE.dataframe_sorted_inst3['org_ip_id'].astype(str).apply(lambda x: str(x).replace(".0", "")).copy()
        
        # NEW_PIA_DF.dataframe = NEW_PIA_DF.dataframe.sample(5000) # debug
        
        print(f'[{os.path.basename(__file__)}/{datetime.now()}]: PIA cleansing running')
        current_time = datetime.now().strftime("%H:%M")
        
        main_num_chunks = m.ceil(len(NEW_PIA_DF.dataframe) / 27500)
        main_chunk_size = len(NEW_PIA_DF.dataframe) // main_num_chunks
        main_start_indices = list(range(0, len(NEW_PIA_DF.dataframe), main_chunk_size))
        main_chunks = [main_chunk.reset_index(drop=True) for main_chunk in [NEW_PIA_DF.dataframe[start_index:start_index+main_chunk_size] for start_index in main_start_indices]]

        # Add the remaining rows to the last chunk
        main_remaining_rows = NEW_PIA_DF.dataframe[main_start_indices[-1]+main_chunk_size:]
        main_chunks[-1] = pd.concat([main_chunks[-1], main_remaining_rows])

        # Process each chunk using concurrent.futures
        print(f'[{os.path.basename(__file__)}/{datetime.now()}]: starting concurrent for main file of {len(NEW_PIA_DF.dataframe)} rows: chunk size is {main_chunk_size}, {main_num_chunks} chunks')
        df_chunks = []
        counter = 0
        for df_chunk in main_chunks:
            counter += 1

            # Calculate the number of chunks and the size of each chunk
            num_chunks = m.ceil(0.8*os.cpu_count()) # use index; e.g, 0 = 1 chunk, 1 = 2 chunks, etc.

            chunk_size = len(df_chunk) // num_chunks
            if chunk_size == 0 or len(df_chunk) == 0:
                break

            # Create a list of starting indices for each chunk
            start_indices = list(range(0, len(df_chunk), chunk_size))

            # Create a list of chunks
            chunks = [chunk.reset_index(drop=True) for chunk in [df_chunk[start_index:start_index+chunk_size] for start_index in start_indices]]

            # Add the remaining rows to the last chunk
            remaining_rows = df_chunk[start_indices[-1]+chunk_size:]
            chunks[-1] = pd.concat([chunks[-1], remaining_rows])

            # Process each chunk using concurrent.futures
            print(f'[{os.path.basename(__file__)}/{datetime.now()}]: starting concurrent ({counter-1}/{main_num_chunks}), sub-chunk size is {chunk_size}')
            with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                results = [pool.apply_async(_chunk_process_, args=(chunk, INVESTOR_TABLE, SECURITY_TABLE)) for chunk in chunks]
                processed_results = [result.get() for result in results]

            print(f'[{os.path.basename(__file__)}/{datetime.now()}]: concurrent for sub-chunk ({counter}/{main_num_chunks}) finished')

            # Combine the processed chunks
            df_chunks.append(pd.concat(processed_results))

        # Combine the processed chunks
        processed_df = pd.concat(df_chunks)
        print(f'[{os.path.basename(__file__)}/{datetime.now()}]: concurrent finished, merging files')
    
    except (KeyboardInterrupt):
        raise (KeyboardInterrupt)
    
    else:
        # post process

        for col in ['msr_prd_id', 'RID', 'BOP_item', 'issuer_countryID', 'scr_currencyID', 'scr_termID', 'end_ivstr_unq_id_tp', 'pvr_role_rspn', 'org_ip_id']:
            if col == 'RID':
                processed_df['RID'] = processed_df['RID'].astype(str).apply(get_zfill)
            processed_df[col] = processed_df[col].astype(str).apply(replace_values)
            
        YEARLY_PIA_DF.dataframe = pd.concat([processed_df, YEARLY_PIA_DF.dataframe], ignore_index=True).replace(['nan', '0000000000nan',  '000000000None'], np.nan)
        YEARLY_PIA_DF.dataframe.reset_index()
        YEARLY_PIA_DF.dataframe['msr_prd_id'] = YEARLY_PIA_DF.dataframe['msr_prd_id'].astype(str)
        YEARLY_PIA_DF.sort(sort_by_var='msr_prd_id')
        for col in ['msr_prd_id', 'RID', 'BOP_item', 'issuer_countryID', 'scr_currencyID', 'scr_termID', 'end_ivstr_unq_id_tp', 'pvr_role_rspn', 'org_ip_id']:
            if col == 'RID':
                YEARLY_PIA_DF.dataframe['RID'] = YEARLY_PIA_DF.dataframe['RID'].astype(str).apply(get_zfill)
            YEARLY_PIA_DF.dataframe[col] = YEARLY_PIA_DF.dataframe[col].astype(str).apply(replace_values).replace(['nan', '0000000000nan',  '000000000None'], np.nan)
        YEARLY_PIA_DF.export()
        # YEARLY_PIA_DF.export(formt='excel')
        
        PIA_PROCESS_CACHE = cache(name='PIA_PROCESS_CACHE', data=[YEARLY_PIA_DF, NEW_PIA_DF, INVESTOR_TABLE, SECURITY_TABLE])
        
        print(f'[{os.path.basename(__file__)}/{datetime.now()}]: Start time = {start_time}, Finished time = {datetime.now()}')
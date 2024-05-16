from imports import *
from sys_functions import *
from sys_class import *

# Generate term for the security
def termValidate(df):
    df['scr_termID'] = 310018
    df.loc[df['tdiff_months'].between(0.0, 12.0), 'scr_termID'] = 310001
    return df

# Generate timediff to calculate term
def timediff(df):
    try:
        df['issu_dt'] = df['issu_dt'].astype(str)
        df['issu_dt'] = pd.to_datetime(df['issu_dt'], errors='coerce')
        df['mat_dt'] = df['mat_dt'].astype(str)
        df['mat_dt'] = pd.to_datetime(df['mat_dt'], errors='coerce')
        df['tdiff_months'] = ((df['mat_dt'].dt.year - df['issu_dt'].dt.year) * 12 + (df['mat_dt'].dt.month - df['issu_dt'].dt.month))

    except ValueError as e:
        raise ValueError(f"Error: {e}. Please check and ensure consistent datetime formats in your data.")
    return df

# ctc/ccy mapping
def map_table(df):
    ctc = pd.read_csv("./source_files/ctc.csv")
    ccy = pd.read_csv("./source_files/ccy.csv")
    dfx = df.merge(ctc, left_on='rgst_cty_id', right_on='Code', how='left')
    dfx = dfx.merge(ccy, left_on="ccy_id", right_on='CCY', how='left')
    return dfx

def fname(file):
    if 'dsr' in file:
        return '264000222'
    elif 'ffr' in file:
        return '264000220'
    elif 'esr' in file:
        return '264000219'
    else:
        return 'unknown'
    
def fname2(df, name):
    if 'dsr' in name or 'esr' in name:
        return df['issu_id']
    elif 'ffr' in name:
        return df['ast_mgt_unq_id']
    else:
        return 'unknown'
    
# main
if __name__ == '__main__':
    try:
        df_lst = []

        try:
            SR_cache = cache(name='SR', data=pd.read_csv('./source_files/SR_N.csv'))
            df_lst.append(SR_cache.data)
        except:
            pass
        
        # edit SR files to import here
        try:
            for sr_name in ['sec_reg_dsr', 'sec_reg_esr', 'sec_reg_ffr']:
                df = df_finds(keyword=sr_name)
                df['type'] = fname(sr_name)
                df['loc_val'] = fname2(df, sr_name)
                df_lst.append(df)
        except:
            for sr_name in [
                # removed due to BOT file names concerned
            ]:
                try:
                    df = hive_import(sr_name)
                    df['type'] = fname(sr_name)
                    df['loc_val'] = fname2(df, sr_name)
                    df_lst.append(df)
                    print(f'{sr_name} loaded')
                                    
                except (Exception):
                    try:
                        sr_fp = [sr_name+'.csv', sr_name+'.xlsx']
                        for path in sr_fp:
                            try:
                                df = pd.read_csv(f'./source_files/{path}')
                                df['type'] = fname(sr_name)  # Add a new column with the file name
                                df['loc_val'] = df[fname2]
                                df_lst.append(df)
                                print(f'{sr_name} loaded')
                                break
                            except:
                                try:
                                    df = pd.read_excel(f'./source_files/{path}')
                                    df['type'] = fname(sr_name) # Add a new column with the file name
                                    df['loc_val'] = df[fname2]
                                    df_lst.append(df)
                                    print(f'{sr_name} loaded')
                                    break
                                except:
                                    print(f'{sr_name} not ok, missing {path}')
                                    continue
                    except:
                        continue

        try:
            merged_df = pd.concat(df_lst) # merge and remove all dupes
            dfa = merged_df.drop_duplicates()
            df = dfa[['scr_code', 'scr_tp_id', 'issu_dt', 'mat_dt', 'rgst_cty_id', 'ccy_id', 'type', 'loc_val']] # select only some columns
            # if DSR/ESR, set new column of 'loc_val' from 'issuer unique id' column
            # if FFR, set new column of 'loc_val' from 'asset' column
            
            # cleanse + prep functions
            df = map_table(df)
            df = timediff(df)
            df = termValidate(df)

            # Leave only required columns + export
            print(list(df.columns))
            df.drop(['rgst_cty_id','ccy_id','CTY_DTL_NM', 'CCY','Code','Name'], axis=1, inplace=True)
            df = df.rename(columns={'CCY_ID': 'scr_currencyID', 'CL_ID': 'issuer_countryID'})
            # df.to_csv("./source_files/SR_N_ORG.csv", index=False)

            # Drop duplicates by scr_code
            df_cleaned = df.drop_duplicates(subset='scr_code').dropna(thresh=1)
            df_cleaned.to_csv("./source_files/SR_N.csv", index=False)
        
        except ValueError:
            print('sr_prep.py: No files to merge')

    except (KeyboardInterrupt):
        raise KeyboardInterrupt()

    except:
        traceback_log()
        
    else:
        finished(os.path.basename(__file__))
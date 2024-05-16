from imports import *
from sys_functions import *

def name_getter():
    config = configparser.ConfigParser()
    config.read('./config.ini')
    return config.get('PIA_Import','imported_file_name')

def file_getter():
    config = configparser.ConfigParser()
    config.read('./config.ini')
    return config.get('PIA_Import','import_table').split(',')

# main
if __name__ == '__main__':

    try:
        df_lst = []

        # edit PIA files to import here
        for pia_name in file_getter():
            try:
                df = hive_import(pia_name, caller="pia_import", custom_name=pia_name)
                df_lst.append(df)
                print(f'{pia_name} loaded')

            except (Exception):
                traceback_log()
                raise Exception('Error occursed during download')

        try:
            # pre-process PIA file
            print('merging files')
            merged_df = pd.concat(df_lst)
            # merged_df.to_csv(f"./input/{name_getter()}_raw.csv", index=False) # for debugging purposes
            merged_df['end_ivstr_id'] = merged_df['end_ivstr_id_new'].astype(str).apply(lambda x: x.zfill(13))
            merged_df.drop('end_ivstr_id_new', axis=1, inplace=True)
            
            # export
            merged_df.to_csv(f"./input/{name_getter()}.csv", index=False)
            print('merged')
        
        except ValueError:
            print('pia_prep.py: No files to merge')

    except:
        traceback_log()
        
    else:
        finished(os.path.basename(__file__))
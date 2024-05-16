from imports import *
from sys_functions import *
from sys_class import *

# main
if __name__ == '__main__':
    try:
        df_lst = []

        try:
            ISR_cache = cache(name='ISR', data=pd.read_csv('./source_files/ISR_N.csv'))
            df_lst.append(ISR_cache.data)
        except:
            pass
        
        # edit ISR files to import here
        try:
            df = df_finds(keyword='issuer_reg')
            # ignore comma in quote
            if len(df) != 0:
                df_lst.append(df)
        except:
            for sr_name in [ 
                            # removed due to BOT file name concerned
                            ]:
                
                try:
                    df = hive_import(sr_name)
                    df_lst.append(df)
                    print(f'{sr_name} loaded')
                    
                except (Exception):
                    traceback_log()
                    try:
                        sr_fp = [sr_name+'.csv', sr_name+'.xlsx']
                        for path in sr_fp:
                            try:
                                df = pd.read_csv(f'./input/{path}')
                                df_lst.append(df)
                                break
                            except:
                                try:
                                    df = pd.read_excel(f'./input/{path}')
                                    df_lst.append(df)
                                    break
                                except:
                                    continue
                    except:
                        continue

        try:
            # pre-process ISR file
            merged_df = pd.concat(df_lst)
            merged_df = merged_df.drop_duplicates(subset='issu_ast_mgt_unq_id').dropna(thresh=1)
            merged_df = merged_df[['nm', 'issu_cty_id', 'issu_ast_mgt_unq_id']]
            
            # export
            merged_df.to_csv("./source_files/ISR_N.csv", index=False)
        
        except ValueError:
            print('isr_prep.py: No files to merge')

    except:
        traceback_log()
        
    else:
        finished(os.path.basename(__file__))
from imports import *
from sys_functions import *
import warnings
from pandas.core.generic import SettingWithCopyWarning

warnings.filterwarnings("ignore", category=SettingWithCopyWarning)

def BRsource(sheet):
    if 'ในประเทศ' in sheet:
        return 'foreigner'
    elif 'ต่างด้าว' in sheet:
        return 'local'

if __name__ == '__main__':

    try:
        selected_dfs = [] # list to store selected columns from each df

        for sheet in ['RID ในประเทศ', 'RID ต่างด้าว']:
            try:
                df = df_finds(keyword='business_reg')
            except:
                df = pd.read_excel('./input/BR.xlsx', sheet_name=sheet)

            if len(df) == 0:
                pd.read_excel('./input/BR.xlsx', sheet_name=sheet)
                
            df['source'] = BRsource(sheet)
            selected_df = df[["RID", 'NAME', 'ISIC', 'IP_ID_BR' ,"SECTOR"]]
            selected_df.rename(columns={'ISIC': 'ISIC4_CLID',
                                        'NAME' : 'end_ivstr_nm',
                                        # 'IP_TP_CODE \n(SNA 2008: 443…)': "SECTOR"
                                        }, inplace=True)
            selected_dfs.append(selected_df)

        # merge all selected df into one df
        merged_df = pd.concat(selected_dfs, ignore_index=True)
        merged_df = merged_df.drop_duplicates()
        merged_df = merged_df.applymap(lambda x: str(x).replace(".0", ""))
        merged_df['RID'] = merged_df['RID'].astype(str).apply(lambda x: x.zfill(13))
        merged_df.to_csv("./source_files/BR_N.csv", index=False)
        
    except:
        traceback_log()
        
    else:
        finished(os.path.basename(__file__))
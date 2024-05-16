from imports import *

import warnings
from pandas.core.generic import SettingWithCopyWarning
import pandas.errors

warnings.filterwarnings("ignore", category=pandas.errors.DtypeWarning)

# Works on Windows only (windows popup for file selection)
'''def window_load_file(requestFileName=False):
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    file_name = os.path.basename(file_path)
    if requestFileName:
        return file_path, file_name
    else:
        return file_path'''


def finished(caller='Unknown', custom_texts=None):
    if custom_texts is None:
        print(caller+": runs completed\n")
    else:
        print(caller+": runs completed (Note: "+custom_texts+")\n")


def read_cache(cache_name):
    with open(f'./cache/{cache_name}.cache', 'rb') as file:
        return pickle.load(file)
    

def get_all_files(path, keyword, any=False):
    file_names = []
    for root, _, files in os.walk(path):
        for file_name in files:
            if keyword in file_name:
                file_names.append(file_name)
    return file_names


def get_zfill(x):
    return str(x).zfill(13)


def replace_values(value):
    return str(value).replace(".0", "")
        
        
# Read existing csv file (generate if doesn't exist)
def df_read(name, path='./output/', clone=None):
    try:
        df = pd.read_csv(path+name+'.csv')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df = pd.DataFrame(columns=clone.columns)
        df.to_csv(path+name+'.csv', index=False)
    return df


# Find .csv from keyword (use only the first file)
def df_finds(keyword, path='./input/', requiredName=False):
    try:
        files = get_all_files(path=path, keyword=keyword)
        if len(files) > 0:
            file = files[0]
            if '.csv' in file:
                df = pd.read_csv(path+file)
            else:
                df = pd.read_excel(path+file)
        else:
            file = None
            df = pd.DataFrame()
    except (FileNotFoundError, pd.errors.EmptyDataError):
        raise Exception(f"DF {keyword} not found")
    else:
        if requiredName:
            return file, df
        else:
            return df
        
        
# sort df (generally used for other df)
def sort_df(df, sort_by, drop_dupe = False, drop_na = False):
    if drop_na:
        df = df.dropna()
        df.reset_index(drop=True, inplace=True)
    if drop_dupe:
        df = df.drop_duplicates(subset='RID')
        df.reset_index(drop=True, inplace=True)
    df = df.sort_values(by=sort_by)
    df.reset_index(drop=True, inplace=True)
    return df

# convert df (or column)
def convert_to(df, Type):
    if Type=='int':
        df = df.fillna('NAN')
        for col in df.columns:
            df[col] = df[col].map(lambda x: int(x) if not pd.isna(x) and x != 'NAN' else x)
        df = df.mask(df == 'NAN', np.nan)
    elif Type=='str':
        for col in df.columns:
            if df[col].dtype != 'object':  # Check if the column is not already a string
                df[col] = df[col].apply(lambda x: str(int(float(x))).strip('.') if isinstance(x, (int, float)) and not np.isnan(x) else np.nan)
    else:
        for col in df.columns:
            df[col] = df[col].map(lambda x: Type(x) if not np.isna(x) else x)
    return df

            
def search_sorted(value, lst, df, placeholder=True):
    if placeholder:
        newRow = pd.DataFrame(columns=df.columns)
    try: 
        indx = lst.searchsorted(value)
    except (TypeError):
        indx = -1
    if indx < len(lst) and indx != -1 and lst.iat[indx] == value:
        newRow = df.iloc[[indx]].reset_index(drop=True) 
    elif str(value) in list(set(lst.astype(str).values)):
        newRow = df[lst.astype(str).isin([str(value)])].iloc[[0]].reset_index(drop=True) 
        
    return newRow            


try: # import from hive function
    import pyodbc
    
    def hive_import(name, caller=None, custom_name=None):
        
        root = # removed due to BOT file names concerned

        def as_pandas_DataFrame(cursor):
            names = [metadata[0] for metadata in cursor.description]
            return pandas.DataFrame([dict(zip(names, row)) for row in cursor], columns=names)

        def configGetter(cfg_path):
            config = configparser.ConfigParser()
            config.read(cfg_path)

            DSN = config.get('Database', 'DSN')
            host = config.get('Database', 'host')
            database = config.get('Database', 'database')
            port = config.get('Database', 'port')
            username = config.get('Database', 'username')
            password = config.get('Database', 'password')

            return DSN, host, database, int(port), username, password

        # Start
        pd.options.mode.chained_assignment = None
        DSN, host, database, port, username, password = configGetter('./config.ini')

        cfg = {'DSN': DSN, 'host': host,
                'database': database,'port': port,
                'username': username, 'password': password}

        conn_string = "DSN=%s; Host=%s; Database=%s; Port=%d; AuthMech=3; UseSASL=1; UID=%s; PWD=%s; SSL=0; AllowSelfSignedServerCert=1" % (cfg['DSN'], cfg['host'],cfg['database'] ,cfg['port'], cfg['username'], cfg['password'])

        conn = pyodbc.connect(conn_string, autocommit=True)
        cur = conn.cursor()

        # main
        if caller is None:
            code = f"SELECT *  \
            FROM {database}.{name} \
            "
            
        else:
            code = f"SELECT *, \
            CAST(end_ivstr_id AS STRING) AS end_ivstr_id_new \
            FROM {database}.{custom_name} \
            "

        cur.execute(code)
        imported_df = as_pandas_DataFrame(cur)

        return imported_df
    
except (ModuleNotFoundError):
    pass


def clear_cache(keywords=None):
    cache_dir = './cache/'
    file_list = glob.glob(cache_dir + '*')
    if keywords is None:
        for file in file_list:
            if os.path.isfile(file):
                os.remove(file)
    else:
        for keyword in keywords:
            for file in file_list:
                if os.path.isfile(file) and keyword in file:
                    os.remove(file)
            
            
def traceback_log():
    with open(f"./errors_{date.today()}.log", "a") as logfile:
        logfile.write(f"\n\n >> DT:{datetime.now()}\n")
        traceback.print_exc(file=logfile)
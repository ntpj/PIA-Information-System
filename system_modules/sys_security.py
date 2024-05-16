from imports import *
from sys_functions import *
from sys_class import *

warnings.filterwarnings('ignore', category=RuntimeWarning, module='icu')

# Security Table
# scrape > 3DB
#        > DSR, ESR  (issuer unique id) > ISR
#        > FFR      > (asset) > ISR

def format_check(frame, tp='row', force_BOP_replacement=True):
    format_dict = {
        'scr_code' : 'raw_str',
        'BOP_item' : 'str',
        'issuer_name' : 'raw_str',
        'issuer_countryID' : 'str',
        'scr_currencyID' : 'str',
        'scr_termID' : 'str',
        'scr_instrument' : 'raw_str'
    }
    if tp == 'row':
        for column in frame.columns:
            if pd.isna(frame.loc[0, column]):
                continue
            if format_dict[column] == 'str':
                frame.loc[0, column] = str(frame.loc[0, column]).replace(".0", "")
            elif format_dict[column] == 'int':
                frame.loc[0, column] = int(float(frame.loc[0, column]))
            elif format_dict[column] == 'raw_str':
                frame.loc[0, column] = str(frame.loc[0, column])
    elif tp == 'df':
        for column in frame.columns:
            if format_dict[column] == 'str':
                frame[column] = frame[column].apply(lambda x: str(x).replace(".0", ""))
            elif format_dict[column] == 'int':
                frame[column] = frame[column].apply(lambda x: int(float(x)))
            elif format_dict[column] == 'raw_str':
                frame[column] = frame[column].apply(lambda x: str(x))
        frame = frame.replace('nan', np.nan)
        frame = frame.drop_duplicates(subset=['scr_code'], keep='first')
        if force_BOP_replacement:
            frame.loc[(frame['scr_instrument'] == 'Stock'), 'BOP_item'] = 264000219
            frame.loc[(frame['scr_instrument'] == 'ETF'), 'BOP_item'] = 264000220
            frame.loc[(frame['scr_instrument'] == 'Bond'), 'BOP_item'] = 264000222
    return frame

# locate termid
def termID_loc(SR_N, scr_code):
    row = search_sorted(df=SR_N.dataframe,
                        lst=SR_N.dataframe['scr_code'].astype(str),
                        value=str(scr_code))
    if len(row) > 0:
        return row.loc[0, 'scr_termID']
    else:
        return 310018 # default to long

# map country/currencyid
def mapper(val, mp, fuzzy=False, ct_by_code=True, custom_ccy_file=None, custom_ctc_file=None, sort_custom=False, sort_custom_by=False):
    try:
        global CCY
        global CTC_BY_CODE
        global CTC
    except (NameError):
        for org_file, custom_file in [(CCY, custom_ccy_file), (CTC, custom_ctc_file)]:
            if custom_file is not None:
                org_file = custom_file
                if sort_custom:
                    sort_df(df = org_file, sort_by=sort_custom_by)
    
    if mp == 'country':
        if ct_by_code:
            if fuzzy:
                row = search_sorted(df=CTC_BY_CODE, lst=CTC_BY_CODE['Code'].astype(str), value=str(countrynames.to_code(val)))
            else:
                row = search_sorted(df=CTC_BY_CODE, lst=CTC_BY_CODE['Code'].astype(str), value=str(val))
            if len(row) > 0:
                return row.loc[0, 'CL_ID']
            else:
                return '999999' # default to others
            
        else:
            row = search_sorted(df=CTC, lst=CTC['Name'].astype(str), value=str(val))
            if len(row) > 0:
                return str(row.loc[0, 'CL_ID']).replace(".0", "")
            else:
                return '999999' # default to others
        
    elif mp == 'currency':
        row = search_sorted(df=CCY, lst=CCY['CCY'].astype(str), value=str(val))
        if len(row) > 0:
            return str(row.loc[0, 'CCY_ID']).replace(".0", "")
        else:
            return '998999' # default to others
        
# ICMA file download
def ICMA_download(url, path, fname):
    response = requests.get(url)
    if response.status_code == 200: # OK = loadable
        with open(path, 'wb') as f:
            f.write(response.content)
    else:
        pass

    # Pre-process ICMA raw .xlsx
    df = pd.read_excel(path)
    # remove col 1, row 1-2
    df = df.iloc[:, 1:] 
    df = df.iloc[2:, :]
    # Assigning the first row as column headers, drop the first row (used as column headers)
    df.columns = df.iloc[0]
    df = df.iloc[1:, :]
    df = df.reset_index(drop=True) # reset index

    for col in df.columns: # remove NaN column
        if isinstance(col, float) and pd.isna(col): 
            df.drop(columns=[col], inplace=True)
            break

    # export to processed, usable IssuerInfo-ICMA file
    df = df.loc[:,['Issuer Name','Issuer location', 'Issuer sector']]
    df.columns = [['Issuer Name','Country Name', 'ISIC']]
    df.to_csv(f"./source_files/IssuerInfo-ICMA-{date.today()}.csv", index=False)

# ICMA file pre-process
def ICMA_reader():
    try:
        # If ICMA file exists and is downloaded today then skip downloading
        if (os.path.exists(f"./source_files/IssuerInfo-ICMA-{date.today()}.csv") 
            and str(date.today()) in f"./source_files/IssuerInfo-ICMA-{date.today()}.csv"):
            ICMA = pd.read_csv(f"./source_files/IssuerInfo-ICMA-{date.today()}.csv")

        # else if file doesn't exists of date not in ICMA file
        elif (not os.path.exists(f"./source_files/IssuerInfo-ICMA-{date.today()}.csv") 
              or str(date.today()) not in f"./source_files/IssuerInfo-ICMA-{date.today()}.csv"):

            for file in os.listdir("./source_files"): # check all files in source files, remvoe all ICMA files to remove dupe
                if "ICMA" in file:
                    os.remove(os.path.join("./source_files",file))

            # define and download ICMA
            icma_raw_file = f'ICMA-Export.xlsx'
            icma_raw_file_path = os.path.join(f"./source_files/", icma_raw_file)
            url = 'https://dl.luxse.com/dl?v=UNyZt0YQgd4QYK9qm97NqmzuJhgb/t+d4UwYshw7WsAJtSrgXqowUKvwuVCtUjCnJUBaHW8J5aQ+XUE1lSxy1irf3GVJCqdnD9J2riPXLF5JLxnN1jXyJ2NxdPCOxFgEs4Ilr31f9kcK1YDxH/DhIxciV/c3XAzleXfAWinuBzw=' 
            ICMA_download(url, icma_raw_file_path, icma_raw_file)
            ICMA = pd.read_csv(f"./source_files/IssuerInfo-ICMA-{date.today()}.csv")

            try:
                os.remove(f'./source_files/{icma_raw_file}')
            except (FileNotFoundError):
                pass

        return ICMA

    except (ConnectionError):
        raise Exception("Sustainable Bond Issuer ICMA file not found")

 
# attempt to map info from scraping
def scrape(new_row):

    # anti-spam filter remover
    def _driver_spamfilter_remover(): # Anti-bot content remover
        global driver
        try:
            elements = driver.find_elements(By.CSS_SELECTOR,'[class*="callForRegistration"]')
            elements.extend(driver.find_elements(By.CSS_SELECTOR,'[class*="callFor"]'))
            for e in elements:
                try: # DELETE REGISTRATION ELEMENT
                    driver.execute_script("""
                    var element = arguments[0];
                    element.parentNode.removeChild(element);
                    """, e)
                except:
                    continue
        except:
            pass
        
    # driver search
    def _driver_search(key): # Search in main search box
        global driver
        TimeOutCount = 0
        while True:
            try:
                delay = 1+TimeOutCount
                WebDriverWait(driver, delay).until(EC.presence_of_element_located((By.ID,'mainSearch')))
                _driver_spamfilter_remover()
                driver.find_element(By.ID, 'mainSearch').send_keys(key)
                time.sleep(3+TimeOutCount)
                break
            except TimeoutException:
                TimeOutCount += 1
                if TimeOutCount > 9:
                    # exit
                    pass
                       
    # extract name             
    def _driver_ext_name(new_row):
        global driver
        global companyhref
        companyhref = np.NaN

        try:
            time.sleep(7)
            if "/stock" in driver.current_url:
                hasName = False
                new_row.loc[0, 'scr_instrument'] = "Stock"
                try:
                    _driver_spamfilter_remover()
                    if (
                        driver.find_element(
                            By.XPATH,
                            '//*[@id="stockEmitent_profile"]/div[2]/ul/li[1]/div[1]/span',
                        ).text
                        == "Issuer"
                    ):
                        new_row.loc[0, 'issuer_name'] = (driver.find_element(By.XPATH, '//*[@id="cb_stock_emitent_full_link"]/a').text)
                        hasName = True
                except:
                    try:
                        _driver_spamfilter_remover()
                        if (
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="stockEmitent_profile"]/div[2]/ul/li[2]/div[1]/span',
                            ).text
                            == "Full borrower / issuer name"
                        ):
                            new_row.loc[0, 'issuer_name'] = (driver.find_element(
                                By.XPATH, '//*[@id="cb_stock_emitent_full_name"]'
                            ).text)
                            hasName = True
                    except NoSuchElementException:
                        pass

                # Try to get country name
                if hasName:
                    _driver_spamfilter_remover()
                    temp = driver.find_element(By.XPATH, '//*[@id="stockCountryName"]').text
                    try:
                        if (
                            driver.find_element(
                                By.XPATH, '//*[@id="stockCountryName"]'
                            ).get_attribute("id")
                            == "stockCountryName"
                            and temp != "-"
                            and temp != np.NaN
                        ):
                            new_row.loc[0, 'issuer_name'] = temp
                    except TypeError:
                        pass
                    
                    try:
                        _driver_spamfilter_remover()
                        companyhref = driver.find_element(
                            By.XPATH, '//*[@id="cb_stock_emitent_full_link"]/a'
                        ).get_attribute("href")
                    except NoSuchElementException:
                        pass

            elif (
                "/etf" in driver.current_url
            ):
                new_row.loc[0, 'scr_instrument'] = "ETF"
                _driver_spamfilter_remover()
                try:
                    new_row.loc[0, 'issuer_name'] = (driver.find_element(
                        By.XPATH, '//*[@id="topPage"]/div[1]/div[10]/div[2]'
                    ).text)
                except NoSuchElementException:
                    new_row.loc[0, 'issuer_name'] = (driver.find_element(
                        By.XPATH, '//*[@id="topPage"]/div[1]/div/div[10]/div[1]'
                    ).text)

            elif (
                "/bond" in driver.current_url
            ):
                new_row.loc[0, 'scr_instrument'] = "Bond"
                _driver_spamfilter_remover()

                Issuer = driver.find_element(
                    By.XPATH, '//*[@id="cb_bond_page_ginfo_resume_emitent"]/div[1]/span'
                ).text
                if Issuer == "Issuer" or Issuer == "Borrower":
                    new_row.loc[0, 'issuer_name'] = (driver.find_element(
                        By.XPATH, '//*[@id="cb_bond_page_ginfo_resume_emitent"]/div[2]/a'
                    ).text)
                    try:  # Try to get href
                        companyhref = driver.find_element(
                            By.XPATH, '//*[@id="cb_bond_page_ginfo_resume_emitent"]/div[2]/a'
                        ).get_attribute("href")
                    except NoSuchElementException:
                        pass
                if (
                    driver.find_element(
                        By.XPATH,
                        '//*[@id="cb_bond_page_ginfo_resume_emitent_full_name"]/div[1]/span',
                    ).text
                    == "Full borrower / issuer name"
                ):
                    new_row.loc[0, 'issuer_name'] = (driver.find_element(
                        By.XPATH,
                        '//*[@id="cb_bond_page_ginfo_resume_emitent_full_name"]/div[2]/span',
                    ).text)
                    
        except (Exception):
            traceback_log()
            pass

        finally:
            return new_row
                            
    # extract currency
    def _driver_ext_currency(new_row):
        
        global driver
        
        def process_txt(inp):
            out = re.sub(r"[^a-zA-Z0-9\s]", "", inp)
            out = out.split()
            return out

        def isCurrency(instrument, title):
            if not pd.isna(instrument):
                for word_part in [process_txt(title), title.split()]:
                    for word in word_part:
                        if ccy.currency(word):
                            currency_t = ccy.currency(word)
                            return currency_t
            return np.NaN
        
        try:
            currency = np.NaN

            loc_mapper = {
                "CLASS_NAME": By.CLASS_NAME,
                "ID" : By.ID
            }
            
            if pd.isna(new_row.loc[0, 'scr_instrument']):
                return new_row

            elif new_row.loc[0, 'scr_instrument'] == "ETF":
                name = "ETF"
                main_searchBy = ["CLASS_NAME", "main-ttl"]
                possibleXP = [
                                '//*[@id="topPage"]/div[1]/div[1]/div[2]/span[2]',
                                '//*[@id="app"]/div[4]/h1'
                            ]
            elif new_row.loc[0, 'scr_instrument'] == "Bond":
                name = "Bond"
                main_searchBy = ["ID", "cb_bond_page_main_ttl"]
                possibleXP = [
                                '//*[@id="toPdf"]/div[2]/ul/li[1]/div[2]',
                                '//*[@id="cb_bond_page_emission_volume"]/div[2]',
                                '//*[@id="cb_bond_page_emission_volume"]/div[2]/span'
                                '//*[@id="toPdf"]/div[2]/ul/li[2]/div[2]',
                                '//*[@id="cb_bond_page_param_volume"]/div[2]',
                                '//*[@id="cb_bond_page_ginfo_resume_outstanding_nominal_price"]/div[2]',
                                '//*[@id="cb_bond_page_ginfo_resume_nominal_price4domestic"]/div[2]',
                                '//*[@id="cb_bond_page_ginfo_resume_placed_volume"]/div[2]',
                                '//*[@id="cb_bond_page_emission_volume"]/div[2]',
                                '//*[@id="cb_bond_page_ginfo_resume_outstanding_nominal_price"]/div[2]/span/span',
                                '//*[@id="cb_bond_page_ginfo_resume_outstanding_nominal_price"]/div[2]/span'
                            ]
            elif new_row.loc[0, 'scr_instrument'] == "Stock":
                name = "Stock"
                main_searchBy = ["ID", "cb_stock_param_1_0"]
                possibleXP = [
                                '//*[@id="cb_exchangeTable_scroll_item_0_9"]/div/div',
                                '//*[@id="cb_exchangeTable_scroll_item_0_4"]/div/div',
                                '//*[@id="cb_exchangeTable_scroll_item_0_8"]/div/div',
                            ]
            for pXP in possibleXP:
                try:
                    _driver_spamfilter_remover()  
                    title = driver.find_element(loc_mapper[main_searchBy[0]], main_searchBy[1]).text
                except NoSuchElementException:
                    _driver_spamfilter_remover()
                    try:
                        title = driver.find_element(
                            By.XPATH, pXP
                        ).text
                    except NoSuchElementException:
                        continue
                currency = isCurrency(new_row.loc[0, 'scr_currencyID'], title)
                if not pd.isna(currency):
                    new_row.loc[0, 'scr_currencyID'] = currency
                    return new_row
                
        except (Exception):
            traceback_log()
            pass
        
        finally:
            return new_row
          
    # extract country
    def _driver_ext_country(new_row):

        global driver
        global companyhref

        threshold = 0.50  # 50% similarity percentage
        redirected = False
        
        try:
            # try to map from existing names in the SEC table
            row = search_sorted(df=SEC.dataframe,
                                    lst=SEC.dataframe['issuer_name'],
                                    value=new_row.loc[0, 'issuer_name'])
            if len(row) > 0:
                new_row.loc[0, 'issuer_countryID'] = row.loc[0, 'issuer_countryID']

            elif pd.isna(companyhref):  # No companyhref can be extracted from previous function
                _driver_search(new_row.loc[0, 'issuer_name'])
                _driver_spamfilter_remover()

                try:  # Attempt to skip force registration page / wait until search box is clickable
                    delay = 10
                    WebDriverWait(driver, delay).until(
                        EC.element_to_be_clickable(
                            (By.XPATH, '//*[@id="header"]/div[1]/div[2]/div[2]/ul/li[4]/a')
                        )
                    )
                    driver.find_element(
                        By.XPATH, '//*[@id="header"]/div[1]/div[2]/div[2]/ul/li[4]/a'
                    ).click()
                except TimeoutException:
                    _driver_spamfilter_remover()
                    driver.find_element(
                        By.XPATH, '//*[@id="header"]/div[1]/div[2]/div[2]/ul/li[4]/a'
                    ).click()
                    pass

                try:  # Wait until search result box appeared
                    delay = 1
                    WebDriverWait(driver, delay).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//*[@id="header"]/div[1]/div[2]/div[2]/ul')
                        )
                    )
                except TimeoutException:
                    pass

                try:  # Try to find country name (check top 10 searches)
                    for i in range(1, 10):
                        _driver_spamfilter_remover()
                        name = driver.find_element(
                            By.XPATH,
                            f'//*[@id="header"]/div[1]/div[2]/div[2]/div[5]/ul/li[{i}]/a/span[1]/span',
                        ).text
                        path = driver.find_element(
                            By.XPATH,
                            f'//*[@id="header"]/div[1]/div[2]/div[2]/div[5]/ul/li[{i}]/a',
                        ).get_attribute("href")
                        if (
                            threshold < SequenceMatcher(None, str(new_row.loc[0, 'issuer_name']), str(name)).ratio()
                        ):  # Check if similarity is more than threshold
                            redirected = True
                            break
                except NoSuchElementException:
                    pass

                finally:
                    if redirected:  # If not found then just exit extractCountry (Info[3] is still NaN)
                        _driver_spamfilter_remover()
                        driver.get(path)

            elif not pd.isna(companyhref):  # Else if companyhref is found
                redirected = True
                driver.get(companyhref)

        except (Exception):
            traceback_log()
            pass
        
        else:
            if redirected:
                NInfo = [np.NaN, np.NaN]
                _driver_spamfilter_remover()
                if "/company" in driver.current_url:
                    NInfo[0] = driver.find_element(
                        By.XPATH, '//*[@id="cb_country_page_issuer_name"]/div/div[2]'
                    ).text
                    _driver_spamfilter_remover()
                    try:
                        NInfo2Candidate1 = driver.find_element(
                            By.XPATH,
                            '//*[@id="cb_country_page_country_registration"]/div/div[2]',
                        ).text
                        NInfo2Candidate2 = driver.find_element(
                            By.XPATH, '//*[@id="cb_country_page_country_name"]/div/div[2]'
                        ).text

                    except NoSuchElementException:
                        NInfo2Candidate1 = np.NaN
                        NInfo2Candidate2 = driver.find_element(
                            By.XPATH, '//*[@id="cb_country_page_country_name"]/div/div[2]'
                        ).text

                    try:  # Set default to country of registration
                        if not pd.isna(NInfo2Candidate1):
                            if NInfo2Candidate1 != "-":
                                NInfo[1] = NInfo2Candidate1
                            else:
                                NInfo[1] = NInfo2Candidate2
                    except Exception as e:
                        raise e
                    
                    i = 0
                    for info in ["issuer_name", "issuer_countryID"]:
                        if pd.isna(NInfo[i]):
                            new_row.loc[0, info] = NInfo[i]
                        else:
                            tmp1 = str(new_row.loc[0, info])
                            tmp2 = str(NInfo[i])
                            sim = SequenceMatcher(None, tmp1, tmp2).ratio()
                            if (
                                sim < 0.50 or pd.isna(tmp1)
                            ):  # If Sim < 50% or old record is empty, we replace with the new data from the country page
                                new_row.loc[0, info] = (NInfo[i])
                        i += 1

                elif "/country" in driver.current_url:  # Update Info[2,3]
                    NInfo[0] = driver.find_element(
                        By.XPATH, '//*[@id="cb_country_page_country_name"]'
                    ).text
                    if pd.isna(new_row.loc[0, 'issuer_name']):
                        new_row.loc[0, 'issuer_name'] = (driver.find_element(By.XPATH, '//*[@id="cb_country_page_country_name"]').text)
                    if pd.isna(new_row.loc[0, 'issuer_countryID']) or new_row.loc[0, 'issuer_countryID'] == 999999:  # Is Country so Issuer Name is Country Name
                        # convert this country to country id
                        new_row.loc[0, 'issuer_countryID'] = mapper(val=new_row.loc[0, 'issuer_countryID'], ct_by_code=True, mp='country', fuzzy=True)
                            
        finally:
            return new_row
          
    # bs4 get all links          
    def _soup_get_links(html):
        npath = "none"
        soup = BeautifulSoup(html, "html.parser")

        # EXTRACT HREF CONTENT WHERE HREF STARTS WITH "/etf", ...
        # Note: This will find all <a> syntax in HTML,
        # which can be problematic in the future if structure of the web is change
        etflinks = soup.find_all(
            "a", href=lambda href: href and href.startswith("/etf") and href != "/etf/"
        )
        bondlinks = soup.find_all(
            "a", href=lambda href: href and href.startswith("/bond") and href != "/bonds/"
        )
        stocklinks = soup.find_all(
            "a", href=lambda href: href and href.startswith("/stock") and href != "/stocks/"
        )

        # FOR EVERY TYPE (ETF, BOND, STOCK) LINKS FOUND, IF THE LIST IS NOT EMPTY,
        # HREF PATH IS SET TO THAT OF THE CONTENT IN THE LIST
        for i in [etflinks, bondlinks, stocklinks]:
            if len(i) > 0:
                npath = i[0]["href"]
        return npath
    
    ##### start    
    global driver
    url = "https://cbonds.com/"
    domain = "cbonds"
    driver.get(url)

    # Check whether ISIN can be found on cbonds.com, try to extract links for redirect from the search
    _driver_spamfilter_remover()
    _driver_search(new_row.loc[0, 'scr_code'])
    html_content = driver.page_source
    new_path = _soup_get_links(html_content)

    # If there is a link, try to get/update Info with name and country of the issuer
    # Update Status to either "NF: No Issuer" or "OK" based on the values added to Info
    if new_path != "none":
        driver.get(f'{"https://cbonds.com"}{new_path}')
        
        # extract name
        new_row = _driver_ext_name(new_row)
        
        # extract currency
        new_row = _driver_ext_currency(new_row)
        
        # extract country
        new_row = _driver_ext_country(new_row)
        
    return new_row


# attempt to map info from other backup methods (investpy, icma)
def scrape_backup(new_row):
    
    def _m_investpy(new_row):
        try:
            instrument = pd.DataFrame(investpy.search_stocks(by="isin", value=str(new_row.loc[0, 'scr_code'])))
            instrument_col_loc = ['name', 'currency']
            i = 0
            for empty in ['issuer_name', 'scr_currencyID']:
                if pd.isna(new_row.loc[0, empty]):
                    new_row.loc[0, empty] = instrument.loc[0, instrument_col_loc[i]]
                i += 1
        except RuntimeError:
            pass
        finally:
            return new_row
        
    def _m_icma(new_row): 
        global ICMA                  
        threshold = 0.75
        try:
            for i in range(len(ICMA['Issuer Name'].values)): # For each index based on range of Issuer Name from CTM_ICMA
                new_row.loc[0, 'issuer_name']
                # If issuer name matches more than 75% with the index in issuer name column from CTM_ICMA
                if SequenceMatcher(str(new_row.loc[0, 'issuer_name']), str(ICMA.loc[i, "Issuer Name"])).ratio() > threshold:
                    new_row.loc[0, 'scr_currencyID'] = ICMA.loc[i, "Country Name"] # update country with CTM_ICMA's country
                else: # else continue for every range value in the CTM_ICMA file
                    continue
        finally:
            return new_row
        
    def _m_investpy_ct(new_row):
        try:
            threshold = 0.75
            if pd.isna(new_row.loc[0, 'issuer_name']):
                pass
            else:
                stock_info = investpy.search_stocks(by="name", value=str(new_row.loc[0, 'issuer_name']))
            if not stock_info.empty:
                df = pd.DataFrame(stock_info)
                for i in range(len(df['name'].values)):
                    # If issuer name matches more than 75% with the index in issuer name column from CTM_ICMA
                    if SequenceMatcher(str(new_row.loc[0, 'issuer_name']), df.loc[i, "name"]).ratio() > threshold:
                        new_row.loc[0, 'issuer_countryID'] = df.loc[i, "country"]
                        break
                    else:
                        continue
        except RuntimeError:
            pass
        finally:
            return new_row  
      
    ### main  
    if pd.isna(new_row.loc[0, 'issuer_name']) or pd.isna(new_row.loc[0, 'scr_currencyID']):
        new_row = _m_investpy(new_row)
        if not pd.isna(new_row.loc[0, 'scr_currencyID']):
            new_row.loc[0, 'scr_instrument'] = 'others'
    for fn in [_m_investpy_ct, _m_icma]:
        if pd.isna(new_row.loc[0, 'issuer_countryID']):
            new_row = fn(new_row)
        else:
            break
        if not pd.isna(new_row.loc[0, 'issuer_countryID']):
            new_row.loc[0, 'scr_instrument'] = 'others'
    return new_row


# attempt to map info from security files (ESR/DSR/FFR) an ISR
def SR_map(new_row):
    global SR_N, ISR_N
    
    SR_PIA_dict = {
        'BOP_item' : 'type',
        'issuer_countryID' : 'issuer_countryID',
        'scr_currencyID' : 'scr_currencyID',
        'scr_termID' : 'scr_termID'
    }
    INST_BOP_dict = {
        'Stock' : '264000219',
        'Bond' : '264000222',
        'ETF' : '264000220'
    }
    try:
        if pd.isna(new_row.loc[0, 'scr_code']):
            pass

        else:
            row_sr = search_sorted(df=SR_N.dataframe,
                                lst=SR_N.dataframe['scr_code'].astype(str),
                                value=str(new_row.loc[0, 'scr_code']))
            if len(row_sr) > 0:
                for empty_col in new_row.columns[new_row.iloc[0].isna()].tolist():
                    try:
                        new_row.loc[0, empty_col] = str(row_sr.loc[0, SR_PIA_dict[empty_col]]).replace(".0", "")
                    except:
                        continue
                if pd.isna(new_row.loc[0, 'issuer_name']) or pd.isna(new_row.loc[0, 'issuer_countryID']):
                    row_isr = search_sorted(df=ISR_N.dataframe,
                                            lst=ISR_N.dataframe['issu_ast_mgt_unq_id'].astype(str),
                                            value=str(row_sr.loc[0, 'loc_val']))
                    if len(row_isr) > 0:
                        new_row.loc[0, 'issuer_name'] = str(row_isr.loc[0, 'nm'])
                        new_row.loc[0, 'issuer_countryID'] = str(mapper(val=row_isr.loc[0, 'issu_cty_id'], ct_by_code=True, mp='country'))
                if not pd.isna(new_row.loc[0, 'scr_instrument']) and new_row.iloc[0]['scr_instrument'] != 'others':
                    new_row.loc[0, 'BOP_item'] = INST_BOP_dict[new_row.loc[0, 'scr_instrument']]
    except:
        traceback_log()
             
    finally:
        return new_row

# main
def security_validation(PIA_RECEIVED, SEC_RECEIVED, force_update=False, self_recheck_override=False, force_BOP = True):
    
    # global dataframes for lookup and driver
    global SEC, CTC, CTC_BY_CODE, CCY, SR_N, ISR_N, ICMA, driver

    PIA = copy.copy(PIA_RECEIVED)
    SEC = SEC_RECEIVED
    
    # pre-process (load and sort)
    CTC = sort_df(df= pd.read_csv("./source_files/ctc.csv"), sort_by='CL_ID')
    CTC_BY_CODE = sort_df(df = pd.read_csv("./source_files/ctc.csv"), sort_by='Code')
    CCY = sort_df(df = pd.read_csv("./source_files/ccy.csv"), sort_by='CCY')
    SR_N = DataFrameFile(name='SR_N', dataframe=df_read(name='SR_N', path='./source_files/'))
    ISR_N = DataFrameFile(name='ISR_N', dataframe=df_read(name='ISR_N', path='./source_files/'))
    SEC_CACHE = cache(name="SEC", data=pd.DataFrame({'current_name' : [None], 'current_index' : [None]}))
    ICMA = ICMA_reader() # get ICMA file
    
    # total files:
    # - PIA: input PIA
    # - SEC: security table
    # - SR_N : security files
    # - ISR_N : issuer file
    # - CTC/CCY : country/currency mapping file
    # - ICMA : ICMA source file
    
    try:        
        print(f'{os.path.basename(__file__)}: Security Running {datetime.now()}')

        # pre-processing
        # PIA.dataframe = prep_cols(PIA.dataframe)
        PIA.length = len(PIA.dataframe)
        ISR_N.sort(sort_by_var='issu_ast_mgt_unq_id')
        SR_N.sort(sort_by_var='scr_code')
        SEC.sort(sort_by_var='scr_code')
        SEC.update_set(id=1, keyword='scr_code', inst=0)
        SEC_og_length = len(SEC.dataframe)
        
        # Cache prep
        if SEC_CACHE.data.loc[0, 'current_name'] is None:
            SEC_CACHE.data.loc[0, 'current_name'] = PIA.name
            SEC_CACHE.data.loc[0, 'current_index'] = 0
        else:
            pass
        
        # self check for SECURITY (using only alternative methods) / for debugging
        if self_recheck_override:
            output_sec = None
            for row in range(0, SEC.length):
                print('\n\n\n\ncounter: '+str(row))
                new_row = SEC.dataframe.iloc[[row]].copy() # SEC.dataframe.iloc[row].to_frame().T
                new_row = new_row.reset_index()
                # print(new_row.columns)
                if new_row.iloc[0].isna().any() or pd.isna(new_row.loc[0, 'scr_instrument']):
                    new_row.loc[0, 'issuer_countryID'] = np.NaN
                    new_row.loc[0, 'scr_currencyID'] = np.NaN
                    print(new_row) # debug
                    for fn in [scrape_backup, SR_map]:
                        if new_row.iloc[0].isna().any():
                            print('-------- yes empty') # debug
                            new_row = fn(new_row)
                        else:
                            break
                        
                # format check
                if pd.isna(new_row.loc[0, 'scr_currencyID']):
                    pass
                elif not (str(new_row.loc[0, 'scr_currencyID']).replace(".0", "")).startswith("99"):
                    new_row.loc[0, 'scr_currencyID'] = str(mapper(val=new_row.loc[0, 'scr_currencyID'], mp='currency')).replace(".0", "")
                else:
                    new_row.loc[0, 'scr_currencyID'] = str(new_row.iloc[0]['scr_currencyID']).replace(".0", "")
                
                if pd.isna(new_row.loc[0, 'issuer_countryID']):
                    pass
                elif not (str(new_row.loc[0, 'issuer_countryID']).replace(".0", "")).startswith("99"):
                    new_row.loc[0, 'issuer_countryID'] = str(mapper(val=new_row.loc[0, 'issuer_countryID'], mp='country', fuzzy=True)).replace(".0", "")
                else:
                    new_row.loc[0, 'issuer_countryID'] = str(new_row.iloc[0]['issuer_countryID']).replace(".0", "")
                new_row.loc[0, 'BOP_item'] = str(new_row.iloc[0]['BOP_item']).replace(".0", "")
                # new_row = format_check(new_row)
                # SEC.dataframe.iloc[row] = new_row
                
                print(new_row) # debug
                
                if output_sec is None:
                    output_sec = new_row
                else:
                    output_sec = pd.concat([output_sec, new_row], ignore_index=True)
                    output_sec.drop('index', axis=1, inplace=True) 
                    
            output_sec = sort_df(output_sec, sort_by='scr_code')
            output_sec.to_csv('./output/SECURITY_TABLE.csv', index=False)
            return output_sec
        
        # driver init
        chrome_options = Options()      
        chrome_options.add_argument("--headless")
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_argument("--disable-third-party-cookies")
        driver = webdriver.Chrome(options=chrome_options)
        print(f'{os.path.basename(__file__)}: Driver started, starting from {SEC_CACHE.data.loc[0, "current_index"]} {datetime.now()}')
        
        ### actual security process
        mapped_counter = 0 # used for export every 10th records found
        for row in range(SEC_CACHE.data.loc[0, 'current_index'], PIA.length):
            counter = row
            
            # all records are processed (if called on the same file again)
            if SEC_CACHE.data.loc[0, 'current_index'] == -1:
                break
            
            # if force update, if found a matching row from SEC, remove that row
            if force_update:
                if (len(search_sorted(df=SEC.dataframe,
                                    lst=SEC.dataframe['scr_code'],
                                    value=PIA.dataframe.loc[row, 'scr_code'])) > 0
                    ):
                    SEC.dataframe = SEC.dataframe[SEC.dataframe['scr_code'] != PIA.dataframe.loc[row, 'scr_code']]
            
            # If scr_code in security table or no scr_code (nan), skip
            if (PIA.dataframe.loc[row, "scr_code"] in SEC.set_1 and not force_update):
                ''' # old code for binary search
                    ( 
                                len(search_sorted(df=SEC.dataframe,
                                 lst=SEC.dataframe["scr_code"],
                                 value=PIA.dataframe.loc[row, "scr_code"])) > 0
                        or pd.isna(PIA.dataframe.loc[row, "scr_code"]) 
                    ) '''
                pass
            
            else:
                new_row = pd.DataFrame({
                            'scr_code': [PIA.dataframe.loc[row, 'scr_code']],
                            'BOP_item': [np.NaN],
                            'issuer_name': [np.NaN],
                            'issuer_countryID': [np.NaN],
                            'scr_currencyID': [mapper(val=PIA.dataframe.loc[row, 'ccy_id'], mp='currency')],
                            'scr_termID': [termID_loc(SR_N, PIA.dataframe.loc[row, 'scr_code'])],
                            'scr_instrument': [np.NaN]
                          })

                # locate for each element ni new_row, if all are found in previous methods then break                    
                for fn in [scrape, scrape_backup, SR_map]:
                    if new_row.iloc[0].isna().any():
                        new_row = fn(new_row)
                    else:
                        break
                
                # attempt to convert currency and country to ID
                if not (str(new_row.loc[0, 'scr_currencyID']).replace(".0", "")).startswith("99"):
                    new_row.loc[0, 'scr_currencyID'] = mapper(val=new_row.loc[0, 'scr_currencyID'], mp='currency')
                if not (str(new_row.loc[0, 'issuer_countryID']).replace(".0", "")).startswith("99"):
                    new_row.loc[0, 'issuer_countryID'] = mapper(val=new_row.loc[0, 'issuer_countryID'], mp='country', fuzzy=True)
                
                # formating and join to main table
                mapped_counter += 1
                new_row = format_check(new_row, tp='row', force_BOP_replacement=force_BOP)
                row_ind = SEC.dataframe['scr_code'].astype(str).searchsorted(str(new_row.loc[0, 'scr_code']))
                SEC.dataframe = pd.concat([SEC.dataframe.iloc[:row_ind], new_row, SEC.dataframe.iloc[row_ind:]], ignore_index=True)
                SEC.dataframe.reset_index(drop=True, inplace=True)
                SEC.update_set(id=1, keyword='scr_code', inst=0)
            
            # 100 records export output / save cache
            if (row % 100000 == 0 and row != 0) or mapped_counter == 20:
                print(f'{os.path.basename(__file__)}: Automatic export {datetime.now()}, {row} rows checked')
                mapped_counter = 0
                SEC.sort(sort_by_var='scr_code')
                SEC.dataframe = format_check(SEC.dataframe, tp='df', force_BOP_replacement=force_BOP)
                SEC.update_set(id=1, keyword='scr_code', inst=0)
                
                SEC_CACHE.data.loc[0, 'current_index'] = row
                SEC_CACHE.cached()
                SEC.export()

    except (KeyboardInterrupt):
        if counter > 0:
            SEC_CACHE.data.loc[0, 'current_index'] = row-1
        elif counter <= 0 and counter != -1:
            SEC_CACHE.data.loc[0, 'current_index'] = 0
        SEC_CACHE.cached()
        SEC.dataframe = format_check(SEC.dataframe, tp='df', force_BOP_replacement=force_BOP)
        SEC.sort(sort_by_var='scr_code')
        SEC.export()
        return SEC
    
    except (TimeoutException): # Driver timeout exception, auto restart
        print(f'{os.path.basename(__file__)}: Automatic export {datetime.now()}, {row} rows checked')
        if counter > 0:
            SEC_CACHE.data.loc[0, 'current_index'] = row-1
        elif counter <= 0 and counter != -1:
            SEC_CACHE.data.loc[0, 'current_index'] = 0
        SEC_CACHE.cached()
        SEC.sort(sort_by_var='scr_code')
        SEC.dataframe = format_check(SEC.dataframe, tp='df', force_BOP_replacement=force_BOP)
        SEC.export()
        security_validation(PIA, SEC)
        
    except Exception as e:
        traceback_log()
        raise e
        
    else:
        finished(os.path.basename(__file__))
        if SEC_CACHE.data.loc[0, 'current_index'] == -1:
            print(f'* {os.path.basename(__file__)}: No changes were made to SEC (Security part already processed)')
        elif len(SEC.dataframe) == SEC_og_length:
            print(f'* {os.path.basename(__file__)}: No changes were made to SEC (All securities can be located from SECURITY TABLE)')
        SEC_CACHE.data.loc[0, 'current_index'] = -1
        SEC_CACHE.cached()
        SEC.sort(sort_by_var='scr_code')
        SEC.dataframe = format_check(SEC.dataframe, tp='df', force_BOP_replacement=force_BOP)
        SEC.export()
        # SEC.export(formt='excel')

    finally:
        return SEC
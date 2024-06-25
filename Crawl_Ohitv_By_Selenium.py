"""
@author : hhtrieu0108
github : https://github.com/hhtrieu0108
linkedin : https://www.linkedin.com/in/trieuhh
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from time import sleep

class SQLserver_Import:
    def __init__(self, driver, server, database, uid, pwd):
        self.driver = driver
        self.server = server
        self.database = database
        self.uid = uid
        self.df = None
        self.pwd = pwd
        self.table = None
        self.conn = None

    def connect_server_sql(self):
        import pyodbc
        conn = pyodbc.connect(f'DRIVER={self.driver};'
                              f'SERVER={self.server};'
                              f'DATABASE={self.database};'
                              f'UID={self.uid};'
                              f'PWD={self.pwd}')
        self.conn = conn
        self.cursur = conn.cursor()
        return self.cursur

    def import_sql(self, df, table):
        self.df = df
        self.table = table
        for index, row in self.df.iterrows():
            cleaned_values = [str(value) if pd.notna(value) else None for value in row]
            insert_query = f"INSERT INTO {self.table} ({', '.join(self.df.columns)}) VALUES ({', '.join(['?'] * len(self.df.columns))})"
            self.cursur.execute(insert_query, cleaned_values)
        self.cursur.commit()

    def import_sql_basic(self, df, table):
        df.to_sql(table, self.conn, if_exists='replace', index=False)

    def select_all(self, table):
        self.table = table
        query = f"select * from {self.table}"
        df = pd.read_sql_query(query, con=self.conn)
        return df

    def close(self):
        self.conn.close()


def Crawl_Data_Feature(url):
    driver = webdriver.Edge()

    driver.get(url)
    sleep(10)

    elems_fea = driver.find_elements(By.CSS_SELECTOR,"div[class='data dfeatur'] [href]")
    title_fea = [elem.accessible_name for elem in elems_fea]
    link_fea = [elem.get_attribute('href') for elem in elems_fea]

    data_fea = pd.DataFrame(list(zip(title_fea,link_fea)),columns=['title','link'])
    data_fea['feature'] = 'Yes'
    driver.quit()
    return data_fea


def Crawl_Data_NoFeature():
    driver = webdriver.Edge()
    data = pd.DataFrame(columns=['title','link'])
    kinds = ['phim-chieu-rap','action-adventure','phim-chinh-kich','phim-hai','phim-bi-an','phim-hinh-su','phim-gia-dinh',
            'phim-lang-man','phim-hanh-dong','phim-gia-tuong','sci-fi-fantasy','phim-phieu-luu','phim-hoat-hinh']
    for kind in kinds:
        for i in range(1,100):
            driver.get(f"https://ohitv.net/the-loai/{kind}/page/{i}/")
            sleep(5)
            new_elems = driver.find_elements(By.CSS_SELECTOR,"div[class='data'] [href]")
            if len(new_elems) == 0:
                break
            else:
                new_title = [elem.accessible_name for elem in new_elems]
                new_link = [elem.get_attribute('href') for elem in new_elems]
            new_data = pd.DataFrame(list(zip(new_title,new_link)),columns=['title','link'])
            new_data['kind'] = kind.replace('-',' ')
            data = pd.concat((data,new_data),axis=0,ignore_index=True)
    data['feature'] = 'No'

    driver.quit()

    return data

def Processing_Data(nofeaturen_film,feature_film):
    def return_feature(data):
        if data in feature_film['title'].to_list():
            return 'Yes'
        else:
            return 'No'
    nofeaturen_film['isfeature'] = nofeature_film['title'].apply(return_feature)
    new_data = nofeaturen_film.drop(columns='feature')
    new_data_nodup = new_data.drop_duplicates('title')

    return new_data,new_data_nodup


def Load_data_to_SQLServer(data_1,driver,server,database,userid,password,data_2,table_name_1,table_name_2):
    sql = SQLserver_Import(driver=f'{driver}',
                           server=f'{server}',
                           database=f'{database}',
                           uid=f'{userid}',
                           pwd=f'{password}')
    sql.connect_server_sql()
    
    check_exist_table_query_1 = f"DROP TABLE IF EXISTS {table_name_1};"
    
    
    create_table_query_1 = f"""
    CREATE TABLE {table_name_1} (
        title nvarchar(100),
        link nvarchar(MAX),
        kind nvarchar(20),
        isfeature nvarchar(10)
    )
    """

    check_exist_table_query_2 = f"DROP TABLE IF EXISTS {table_name_2};"

    create_table_query_2 = f"""
    CREATE TABLE {table_name_2} (
        title nvarchar(100),
        link nvarchar(MAX),
        kind nvarchar(20),
        isfeature nvarchar(10)
    )
    """
    sql.cursur.execute(check_exist_table_query_1)
    sql.cursur.execute(create_table_query_1)
    sql.cursur.execute(check_exist_table_query_2)
    sql.cursur.execute(create_table_query_2)
    sql.conn.commit()
    sql.import_sql(data_1,table_name_1)
    sql.import_sql(data_2,table_name_2)
    sql.close()

if __name__ == "__main__":
    feature_film = Crawl_Data_Feature("https://ohitv.net/phim-le/page/1/")
    nofeature_film = Crawl_Data_NoFeature()
    all_film_data,all_film_data_nodup = Processing_Data(nofeature_film,feature_film)
    Load_data_to_SQLServer( data_1=all_film_data,
                            data_2=all_film_data_nodup,
                            driver='your_driver',
                            database='your_databaes',
                            userid= 'your_userid',
                            password= 'your_password',
                            table_name_1='ohitv_film',
                            table_name_2='ohitv_film_nodup')


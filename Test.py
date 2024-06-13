import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

def crawl():
    """
    definition : Crawl the website Ohitv.net to gather information about the various types of films it offers.
    return : dataframe of ohitv
    """

    url = "https://ohitv.net/"
    html = requests.get(url)
    soup = BeautifulSoup(html.text,'html.parser')

    kind_href = []
    kind_menu = soup.find_all('ul',class_='sub-menu')[1].find_all('a',href=True)
    kind_link = [link for link in kind_menu]
    kind_href = [link['href'] for link in kind_link]

    title = []
    film_link = []
    date = []
    rating = []
    quality = []
    genre = []
    short_des = []

    for link in kind_href:
        html = requests.get(link)
        soup = BeautifulSoup(html.text,'html.parser')

        data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
        rating_data = soup.find_all('div',class_='rating')
        quality_data = soup.find_all('div',class_='mepo')
        genre_data = soup.find_all('div',class_='mta')
        short_des_data = soup.find_all('div',class_='texto')

        short_des_1 = [short.text for short in short_des_data]
        short_des.extend(short_des_1)

        for genre_data_sub in genre_data:
            sub_type = [text.text for text in genre_data_sub]
            genre.append(sub_type)

        quality_1 = [quality_param.text for quality_param in quality_data]
        quality.extend(quality_1)

        title_1 = [title_param.find('h3').text for title_param in data_html]
        title.extend(title_1)

        film_link_1 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
        film_link.extend(film_link_1)

        date_1 = [date_param.find('span').text for date_param in data_html]
        date.extend(date_1)
        
        rating_1 = [rating_param.text for rating_param in rating_data]
        rating.extend(rating_1)

        # Find number of the page to requests
        pages = []
        page_list = soup.find_all('div',class_='pagination')
        for page_number in page_list:
            for page_href in page_number.find_all('a',href=True):
                pages.append(page_href['href'])

        # Go to each page to crawl
        for page in pages:
            html = requests.get(page)
            soup = BeautifulSoup(html.text,'html.parser')

            data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
            rating_data = soup.find_all('div',class_='rating')
            quality_data = soup.find_all('div',class_='mepo')
            genre_data = soup.find_all('div',class_='mta')
            short_des_data = soup.find_all('div',class_='texto')

            short_des_2 = [short.text for short in short_des_data]
            short_des.extend(short_des_2)

            for genre_data_sub in genre_data:
                sub_type = [text.text for text in genre_data_sub]
                genre.append(sub_type)

            quality_2 = [quality_param.text for quality_param in quality_data]
            quality.extend(quality_2)

            title_2 = [title_param.find('h3').text for title_param in data_html]
            title.extend(title_2)

            film_link_2 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
            film_link.extend(film_link_2)

            date_2 = [date_param.find('span').text for date_param in data_html]
            date.extend(date_2)

            rating_2 = [rating_param.text for rating_param in rating_data]
            rating.extend(rating_2)
            print(len(short_des))
    
    df = pd.DataFrame(list(zip(title,film_link,date,rating,quality,genre,short_des)),columns=['title','links','date','rating','quality','genre','short_description'])
    return df

def load_data_base(df,username,password,host,database,table_name):
    """
    definition : import to postgres database and save to local an csv file
    """
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}/{database}" # Connect to database
    engine = create_engine(db_connection_string)
    df.to_csv('Ohitv_Film_Requests.csv',index=False)
    df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)

if __name__ == "__main__":
    df = crawl()
    load_data_base(
                    df=df,username='your_username',
                    password='your_password',
                    host='your_host',       
                    database='your_database',
                    table_name='ohitv_request'
                    )

import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine


def get_soup(url):
    html = requests.get(url)
    soup = BeautifulSoup(html.text,'html.parser')
    return soup

def get_url():
    url = "https://ohitv.net/"
    soup = get_soup(url)
    kind_href = []
    kind_menu = soup.find_all('ul',class_='sub-menu')[1].find_all('a',href=True)
    kind_link = [link for link in kind_menu]
    kind_href = [link['href'] for link in kind_link]
    return kind_href

def get_page(soup):
    pages = []
    page_list = soup.find_all('div',class_='pagination')
    for page_number in page_list:
        for page_href in page_number.find_all('a',href=True):
            pages.append(page_href['href'])
    return pages

def crawl_title(kind_href):
    title = []
    for link in kind_href:
        soup = get_soup(link)
        data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
        title_1 = [title_param.find('h3').text for title_param in data_html]
        title.extend(title_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
            title_2 = [title_param.find('h3').text for title_param in data_html]
            title.extend(title_2)
    return title

def crawl_film_link(kind_href):
    film_link = []
    for link in kind_href:
        soup = get_soup(link)
        data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
        film_link_1 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
        film_link.extend(film_link_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
            film_link_2 = [link_param.find('h3').find('a',href=True)['href'] for link_param in data_html]
            film_link.extend(film_link_2)
    return film_link

def crawl_date(kind_href):
    date = []
    for link in kind_href:
        soup = get_soup(link)
        data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
        date_1 = [date_param.find('span').text for date_param in data_html]
        date.extend(date_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            data_html = soup.find('div',class_='items normal').find_all('div',class_='data')
            date_2 = [date_param.find('span').text for date_param in data_html]
            date.extend(date_2)
    return date

def crawl_rating(kind_href):
    rating = []
    for link in kind_href:
        soup = get_soup(link)
        rating_data = soup.find_all('div',class_='rating')
        rating_1 = [rating_param.text for rating_param in rating_data]
        rating.extend(rating_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            rating_data = soup.find_all('div',class_='rating')
            rating_2 = [rating_param.text for rating_param in rating_data]
            rating.extend(rating_2)
    return rating
        
def crawl_quality(kind_href):
    quality = []
    for link in kind_href:
        soup = get_soup(link)
        quality_data = soup.find_all('div',class_='mepo')
        quality_1 = [quality_param.text for quality_param in quality_data]
        quality.extend(quality_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            quality_data = soup.find_all('div',class_='mepo')
            quality_2 = [quality_param.text for quality_param in quality_data]
            quality.extend(quality_2)
    return quality

def crawl_genre(kind_href):
    genre = []
    for link in kind_href:
        soup = get_soup(link)
        genre_data = soup.find_all('div',class_='mta')
        for genre_data_sub in genre_data:
            sub_type = [text.text for text in genre_data_sub]
            genre.append(sub_type)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            genre_data = soup.find_all('div',class_='mta')
            for genre_data_sub in genre_data:
                sub_type = [text.text for text in genre_data_sub]
                genre.append(sub_type)
    return genre

def crawl_short_description(kind_href):
    short_des = []
    for link in kind_href:
        soup = get_soup(link)
        short_des_data = soup.find_all('div',class_='texto')
        short_des_1 = [short.text for short in short_des_data]
        short_des.extend(short_des_1)
        pages = get_page(soup=soup)
        for page in pages:
            soup = get_soup(page)
            short_des_data = soup.find_all('div',class_='texto')
            short_des_2 = [short.text for short in short_des_data]
            short_des.extend(short_des_2)
    return short_des

def convert_to_dataframe(kind_href):
    title = crawl_title(kind_href=kind_href)
    film_link = crawl_film_link(kind_href=kind_href)
    date = crawl_date(kind_href=kind_href)
    rating = crawl_rating(kind_href=kind_href)
    quality = crawl_quality(kind_href=kind_href)
    genre = crawl_genre(kind_href=kind_href)
    short_des = crawl_short_description(kind_href=kind_href)
    df = pd.DataFrame(list(zip(title,film_link,date,rating,quality,genre,short_des)),columns=['title','links','date','rating','quality','genre','short_description'])
    return df

def processing(df):
    new_df = df.copy()
    new_df = new_df.drop_duplicates('title')
    return new_df

def load_to_database(df,username,password,host,database,table_name):
    """
    definition : import to postgres database and save to local an csv file
    """
    db_connection_string = f"postgresql+psycopg2://{username}:{password}@{host}/{database}" # Connect to database
    engine = create_engine(db_connection_string)
    df.to_csv('Ohitv_Film_Requests.csv',index=False)
    df.to_sql(f"{table_name}", engine, if_exists='replace', index=False)

if __name__ == "__main__":
    kind_href = get_url()
    df = convert_to_dataframe(kind_href=kind_href)
    processed_df = processing(df=df)
    load_to_database(
                    df=processed_df,
                    username='your_username',
                    password='your_password',
                    host='your_host',       
                    database='your_database',
                    table_name='ohitv_request'
                    )

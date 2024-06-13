# Ohitv Data Crawling Project

This project aims to crawl data from `Ohitv.net` using two different methods: `Selenium` and `Request API`. The crawled data will be converted into a dataframe containing information such as `title`, `link`, `date`, `rating`, `quality`, `genre`, and a `short description` of the films. Finally, the data will be imported into either a `Postgres` database (using Request API) or an `SQL Server` (using Selenium).

## Objective
The objective of this project is to extract data from Ohitv.net and store it in a structured format for further analysis or use in applications.

## Methodologies
- **`Selenium`**: Utilizing Selenium to navigate and scrape data from Ohitv.net by automating web browser interactions.
- **`Request API`**: Employing API requests to retrieve data directly from Ohitv.net without browser automation.

## Data Extraction
The extracted data will include:
- **`Title`**: The title of the film.
- **`Links`**: Link to the film.
- **`Date`**: Publication date of the film.
- **`Rating`**: The rating score of the film.
- **`Quality`**: Quality of the film.
- **`Genre`**: Genre or category of the film.
- **`Short Description`**: A brief description of the film.

## Database Integration
Depending on the method used, the data will be imported into:
- **`Postgres`**: Utilizing the Request API method to import data into a Postgres database.
- **`SQL` Server**: Utilizing the Selenium method to import data into an SQL Server database.

## Contact
For any queries, suggestions, or collaboration opportunities, feel free to reach out. Your insights and feedback are greatly appreciated, as I'm always eager to learn and improve.

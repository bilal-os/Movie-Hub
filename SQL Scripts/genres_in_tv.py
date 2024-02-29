import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
import pyodbc

tvseries_count=100

# Set your TMDb API key
tmdb.API_KEY = '4eb44307ae530644440954cfafd9d49f'

# Set up a connection to your SQL Server database
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost;"
    "Database=tvhub;"
    "UID=sa;"
    "PWD=123BIL@l789;"
    "TrustServerCertificate=yes;"
)

def fetch_tv_series_page(page_number):
    try:
        discover = tmdb.Discover()
        tv_series_page = discover.tv(page=page_number)
        return tv_series_page['results']
    except Exception as e:
        print(f"Failed to fetch TV series on page {page_number}: {str(e)}")
        return []

def fetch_large_dataset_of_tv_series(total_tv_series, tv_series_per_page=20):
    try:
        # Calculate the number of pages needed
        total_pages = total_tv_series // tv_series_per_page
        if total_tv_series % tv_series_per_page != 0:
            total_pages += 1

        # Fetch TV series for each page and concatenate the results
        all_tv_series = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_tv_series_page, page_number) for page_number in range(1, total_pages + 1)]

            for future in futures:
                tv_series_on_page = future.result()
                all_tv_series.extend(tv_series_on_page)

                # Break the loop if we've reached the desired number of TV series
                if len(all_tv_series) >= total_tv_series:
                    break

        return all_tv_series[:total_tv_series]  # Return the specified number of TV series
    except Exception as e:
        print(f"Failed to fetch a large dataset of TV series: {str(e)}")
        return []

def insert_tv_series_genre_relation_into_db(tv_series_id, genre_name):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Insert TV series-genre relation into the tvseriesgenre table
            cursor.execute("INSERT INTO tvseriesgenre (tv_series_id, genre_name) VALUES (?, ?)", tv_series_id, genre_name)
            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"TV Series-Genre relation for TV Series ID {tv_series_id} and Genre Name {genre_name} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert TV series-genre relation into the database: {str(e)}")

def print_tv_series_genres(tv_series):
    tv_series_id = tv_series['id']
    
    # Fetch TV series details to get genre information
    try:
        tv_series_details = tmdb.TV(tv_series_id).info()
        genres = [genre['name'] for genre in tv_series_details['genres']]
        print(f"TV Series ID: {tv_series_id}, Genres: {genres}")

        # Insert TV series-genre relation into the database
        for genre_name in genres:
            insert_tv_series_genre_relation_into_db(tv_series_id, genre_name)
    except Exception as e:
        print(f"Failed to fetch genres for TV Series ID {tv_series_id}: {str(e)}")

# Fetch a large dataset of TV series concurrently
total_tv_series = tvseries_count
tv_series_to_fetch = fetch_large_dataset_of_tv_series(total_tv_series=total_tv_series)

# Print and insert TV series genres concurrently using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(print_tv_series_genres, tv_series_to_fetch)

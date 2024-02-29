import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import requests
import pyodbc

series_count=100

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

def get_tv_series_details(tv_series_id):
    try:
        tv_series = tmdb.TV(tv_series_id).info()
        title = tv_series['name']
        overview = tv_series['overview']
        poster_path = tv_series['poster_path']
        backdrop_path = tv_series['backdrop_path']
        no_of_seasons = tv_series['number_of_seasons']
        rating = tv_series['vote_average']
        first_air_date = tv_series['first_air_date']

        # Fetch creator information from 'created_by'
        creators = tv_series.get('created_by', [])
        # Assuming the first creator in the list is the TV series creator
        creator = creators[0] if creators else None
        creator_id = creator['id'] if creator else None

        # Fetch trailer information
        trailers = tmdb.TV(tv_series_id).videos()
        # Assuming the first trailer in the list is the one to use
        trailer_id = trailers['results'][0]['key'] if trailers['results'] else None

        current_date = datetime.now()
        first_air_date_datetime = datetime.strptime(first_air_date, '%Y-%m-%d')
        months_difference = (current_date.year - first_air_date_datetime.year) * 12 + current_date.month - first_air_date_datetime.month

        type1 = 'latest' if months_difference <= 5 else 'trending' if rating > 8 else None

        # Construct complete image URLs
        base_image_url = tmdb.Configuration().info()['images']['base_url']
        poster_url = f"{base_image_url}original{poster_path}" if poster_path else None
        backdrop_url = f"{base_image_url}original{backdrop_path}" if backdrop_path else None

        return {
            'id': tv_series_id,
            'title': title,
            'overview': overview,
            'poster_url': poster_url,
            'backdrop_url': backdrop_url,
            'no_of_seasons': no_of_seasons,
            'rating': rating,
            'first_air_date': first_air_date,
            'creator_id': creator_id,
            'trailer_id': trailer_id,
            'type1': type1
        }
    except Exception as e:
        print(f"Failed to get details for TV series {tv_series_id}: {str(e)}")
        return None


def download_image_as_binary(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to download image from {url}: {str(e)}")
        return None


def insert_tv_series_details_into_db(conn, tv_series_details):
    try:
        # Download poster and backdrop images as binary if URLs are available
        poster_url = tv_series_details['poster_url']
        backdrop_url = tv_series_details['backdrop_url']

        if poster_url is not None:
            poster_binary = download_image_as_binary(poster_url)
        else:
            poster_binary = None

        if backdrop_url is not None:
            backdrop_binary = download_image_as_binary(backdrop_url)
        else:
            backdrop_binary = None

        # Check if the poster or backdrop URL is not available
        if poster_binary is None or backdrop_binary is None:
            print(f"Skipping TV series {tv_series_details['id']} due to missing image URL")
            return

        # Insert into the database using parameterized query
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO tvseries (id, title, overview, poster, background, noOfSeason, rating, first_air_date, creatorid, trailerid, type1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tv_series_details['id'],
                tv_series_details['title'],
                tv_series_details['overview'],
                pyodbc.Binary(poster_binary) if poster_binary is not None else None,
                pyodbc.Binary(backdrop_binary) if backdrop_binary is not None else None,
                tv_series_details['no_of_seasons'],
                tv_series_details['rating'],
                tv_series_details['first_air_date'],
                tv_series_details['creator_id'],
                tv_series_details['trailer_id'],
                tv_series_details['type1']
            ))
    except pyodbc.Error as e:
        print(f"Failed to insert TV series details into the database: {str(e)}")
        print(f"Problematic poster URL: {tv_series_details['poster_url']}")
        print(f"Problematic backdrop URL: {tv_series_details['backdrop_url']}")

# Fetch a large dataset of TV series concurrently
tv_series_count = series_count
with ThreadPoolExecutor(max_workers=10) as executor:
    tv_series_to_fetch = fetch_large_dataset_of_tv_series(total_tv_series=tv_series_count)
    futures = [executor.submit(get_tv_series_details, tv_series['id']) for tv_series in tv_series_to_fetch]

    # Establish a connection to the SQL Server
    try:
        conn = pyodbc.connect(conn_str)

        # Insert TV series details into the database concurrently
        with ThreadPoolExecutor(max_workers=10) as insert_executor:
            insert_futures = [insert_executor.submit(insert_tv_series_details_into_db, pyodbc.connect(conn_str), future.result()) for future in futures]

        # Wait for all insert operations to complete
        for insert_future in insert_futures:
            insert_future.result()

    except Exception as e:
        print(f"Failed to connect to the database or insert data: {str(e)}")

    finally:
        # Close the database connection
        if conn:
            conn.close()

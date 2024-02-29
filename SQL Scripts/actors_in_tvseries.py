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

def insert_actor_to_database(actor_id, name):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Insert actor's information into the actor table
            cursor.execute("INSERT INTO actor (id, name) VALUES (?, ?)", actor_id, name)
            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Actor with ID {actor_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert actor to the database: {str(e)}")

def insert_tv_series_actor_relation_into_db(tv_series_id, actor_id):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Insert TV series-actor relation into the actorsintvseries table
            cursor.execute("INSERT INTO actorsintvseries (tv_series_id, actor_id) VALUES (?, ?)", tv_series_id, actor_id)
            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"TV Series-Actor relation for TV Series ID {tv_series_id} and Actor ID {actor_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert TV series-actor relation into the database: {str(e)}")

def get_actor_details(actor_id):
    try:
        # Fetch actor details
        person = tmdb.People(actor_id).info()
        actor_name = person['name']

        # Return actor's name
        return actor_id, actor_name
    except Exception as e:
        print(f"Failed to get actor details: {str(e)}")
        return None, None

def get_tv_series_details(tv_series_id):
    try:
        # Fetch credits to get cast information
        credits = tmdb.TV(tv_series_id).credits()

        # Assuming the first 6 cast members are the main actors
        cast = [actor['id'] for actor in credits.get('cast', [])[:6]]

        return tv_series_id, cast
    except Exception as e:
        print(f"Failed to get TV series details: {str(e)}")
        return None, None

def print_tv_series_details(tv_series):
    tv_series_id, cast = get_tv_series_details(tv_series['id'])
    print(f"TV Series ID: {tv_series_id}")

    if cast:
        with ThreadPoolExecutor() as actor_executor:
            for actor_id in cast:
                actor_id, actor_name = get_actor_details(actor_id)
                if actor_name:
                    print(f"  - Actor ID: {actor_id}, Name: {actor_name}")

                    # Insert actor into the database
                    insert_actor_to_database(actor_id, actor_name)

                    # Insert TV series-actor relation into the database
                    insert_tv_series_actor_relation_into_db(tv_series_id, actor_id)
                else:
                    print(f"  - Actor ID: {actor_id}, Name: Actor details not available")
    else:
        print("No cast information available")

# Function to fetch a page of TV series using the discover endpoint
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

# Fetch a large dataset of TV series concurrently
total_tv_series = tvseries_count
with ThreadPoolExecutor() as executor:
    tv_series_to_fetch = fetch_large_dataset_of_tv_series(total_tv_series=total_tv_series)
    executor.map(print_tv_series_details, tv_series_to_fetch)

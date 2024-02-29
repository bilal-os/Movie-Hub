import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import requests

movies_count = 100

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

def insert_creator_to_database(name, picture_data, creator_id):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if picture_data is None
            if picture_data is not None:
                # Insert creator's name, ID, and image data into the creators table
                cursor.execute("INSERT INTO creators (id, name, picture) VALUES (?, ?, ?)", creator_id, name, pyodbc.Binary(picture_data))
            else:
                # Insert creator's name and ID, with picture set to NULL
                cursor.execute("INSERT INTO creators (id, name, picture) VALUES (?, ?, NULL)", creator_id, name)

            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Creator with ID {creator_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert creator to the database: {str(e)}")


def get_tv_series_details(tv_series_id):
    try:
        # Fetch TV series details
        tv_series = tmdb.TV(tv_series_id).info()
        title = tv_series['name']

        # Fetch creators from the 'created_by' field
        creators = tv_series.get('created_by', [])

        # Return title and a list of creators with their names and IDs
        return title, [{'name': creator['name'], 'id': creator['id']} for creator in creators]
    except Exception as e:
        print(f"Failed to get TV series details: {str(e)}")
        return None, None


def get_creator_details(creator_id):
    try:
        # Fetch creator details
        person = tmdb.People(creator_id).info()
        creator_name = person['name']
        profile_path = person['profile_path']

        # Return creator's name and profile image URL
        return creator_name, profile_path
    except Exception as e:
        print(f"Failed to get creator details: {str(e)}")
        return None, None

def print_tv_series_details(tv_series):
    title, creators = get_tv_series_details(tv_series['id'])
    print(f"Title: {title}")

    if creators:
        print("Creators:")
        with ThreadPoolExecutor() as image_executor:
            for creator in creators:
                creator_id = creator['id']
                creator_name, profile_path = get_creator_details(creator_id)
                if profile_path:
                    image_url = f"https://image.tmdb.org/t/p/original{profile_path}"
                    print(f"  - ID: {creator_id}, Name: {creator_name}, Image Path: {image_url}")

                    # Read the image data directly
                    picture_data = requests.get(image_url).content

                    # Insert creator into the database
                    insert_creator_to_database(creator_name, picture_data, creator_id)
                else:
                    print(f"  - ID: {creator_id}, Name: {creator_name}, Image Path: Image not available")

                    # Insert creator into the database with picture set to NULL
                    insert_creator_to_database(creator_name, None, creator_id)
                    print(f"  - Creator {creator_id} inserted with NULL image.")
    else:
        print("Creator information not available")


# Function to fetch a page of TV series using the discover endpoint
def fetch_tv_series_page(page_number):
    try:
        discover = tmdb.Discover()
        tv_series_page = discover.tv(page=page_number)
        return tv_series_page['results']
    except Exception as e:
        print(f"Failed to fetch TV series on page {page_number}: {str(e)}")
        return []

def fetch_large_dataset_of_tv_series(total_tv_series=movies_count, tv_series_per_page=20):
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

# Fetch a large dataset of TV series (e.g., movies_count TV series)
large_dataset_of_tv_series = fetch_large_dataset_of_tv_series(total_tv_series=movies_count)

# Print TV series details concurrently using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(print_tv_series_details, large_dataset_of_tv_series)
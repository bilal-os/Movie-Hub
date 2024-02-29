import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import requests

movies_count = 500

# Set your TMDb API key
tmdb.API_KEY = '4eb44307ae530644440954cfafd9d49f'

# Set up a connection to your SQL Server database
conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=localhost;"
    "Database=moviehub;"
    "UID=sa;"
    "PWD=123BIL@l789;"
    "TrustServerCertificate=yes;"
)

skipped_records = 0

def insert_actor_to_database(actor_id, name, picture_data):
    global skipped_records  # Declare the counter as global

    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if picture_data is None
            if picture_data is not None:
                # Insert actor's information into the actor table
                cursor.execute("INSERT INTO actor (id, name, picture) VALUES (?, ?, ?)", actor_id, name, pyodbc.Binary(picture_data))
            else:
                # Insert actor's information into the actor table with picture set to NULL
                cursor.execute("INSERT INTO actor (id, name, picture) VALUES (?, ?, NULL)", actor_id, name)

            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Actor with ID {actor_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert actor to the database: {str(e)}")
        skipped_records += 1 



def get_actor_details(actor_id):
    try:
        # Fetch actor details
        person = tmdb.People(actor_id).info()
        actor_name = person['name']
        profile_path = person['profile_path']

        # Return actor's name and profile image URL
        return actor_id, actor_name, profile_path
    except Exception as e:
        print(f"Failed to get actor details: {str(e)}")
        return None, None, None


def get_movie_details(movie_id):
    try:
        # Fetch movie details
        movie = tmdb.Movies(movie_id).info()
        title = movie['title']

        # Fetch credits to get cast information
        credits = tmdb.Movies(movie_id).credits()

        # Assuming the first 6 cast members are the main actors
        cast = [{'id': actor['id'], 'name': actor['name'], 'image_path': actor['profile_path']} for actor in credits.get('cast', [])[:6]]

        return title, cast
    except Exception as e:
        print(f"Failed to get movie details: {str(e)}")
        return None, None

def print_movie_details(movie):
    title, cast = get_movie_details(movie['id'])
    print(f"Title: {title}")

    if cast:
        cast_length = len(cast)
        
        print(f"Cast (Length: {cast_length}):")
        with ThreadPoolExecutor() as image_executor:
            for actor in cast:
                actor_id, actor_name, profile_path = get_actor_details(actor['id'])
                if profile_path:
                    image_url = f"https://image.tmdb.org/t/p/original{profile_path}"
                    print(f"  - ID: {actor_id}, Name: {actor_name}, Image Path: {image_url}")

                    # Read the image data directly
                    picture_data = requests.get(image_url).content

                    # Insert actor into the database
                    insert_actor_to_database(actor_id, actor_name, picture_data)
                else:
                    print(f"  - ID: {actor_id}, Name: {actor_name}, Image Path: Image not available")

                    # Insert actor into the database with picture set to NULL
                    insert_actor_to_database(actor_id, actor_name, None)
                    print(f"  - Actor {actor_id} inserted with NULL image.")

        # Print a message if cast size is not 6
        if cast_length != 6:
            print(f"Full cast information not available or cast size is not 6.")
    else:
        print("Cast information not available")


def fetch_large_dataset_of_movies(total_movies=movies_count, movies_per_page=20):
    try:
        # Calculate the number of pages needed
        total_pages = total_movies // movies_per_page
        if total_movies % movies_per_page != 0:
            total_pages += 1

        # Fetch movies for each page and concatenate the results
        all_movies = []
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(fetch_movies_page, page_number) for page_number in range(1, total_pages + 1)]

            for future in futures:
                movies_on_page = future.result()
                all_movies.extend(movies_on_page)

                # Break the loop if we've reached the desired number of movies
                if len(all_movies) >= total_movies:
                    break

        return all_movies[:total_movies]  # Return the specified number of movies
    except Exception as e:
        print(f"Failed to fetch a large dataset of movies: {str(e)}")
        return []

# Function to fetch a page of movies using the discover endpoint
def fetch_movies_page(page_number):
    try:
        discover = tmdb.Discover()
        movies_page = discover.movie(page=page_number)
        return movies_page['results']
    except Exception as e:
        print(f"Failed to fetch movies on page {page_number}: {str(e)}")
        return []

# Fetch a large dataset of movies (e.g., movies_count movies)
large_dataset_of_movies = fetch_large_dataset_of_movies(total_movies=movies_count)

# Print movie details concurrently using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(print_movie_details, large_dataset_of_movies)

# Print the total number of skipped records
print(f"Total number of skipped records: {skipped_records}")

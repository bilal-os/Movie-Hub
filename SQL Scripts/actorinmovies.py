import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
import pyodbc
import requests

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

def insert_movie_actor_relation_into_db(movie_id, actor_id):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Insert movie-actor relation into the actorsinMovies table
            cursor.execute("INSERT INTO actorsinMovies (movie_id, actor_id) VALUES (?, ?)", movie_id, actor_id)
            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Movie-Actor relation for Movie ID {movie_id} and Actor ID {actor_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert movie-actor relation into the database: {str(e)}")

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

def get_movie_details(movie_id):
    try:
        # Fetch credits to get cast information
        credits = tmdb.Movies(movie_id).credits()

        # Assuming the first 6 cast members are the main actors
        cast = [actor['id'] for actor in credits.get('cast', [])[:6]]

        return movie_id, cast
    except Exception as e:
        print(f"Failed to get movie details: {str(e)}")
        return None, None

def print_movie_details(movie):
    movie_id, cast = get_movie_details(movie['id'])
    print(f"Movie ID: {movie_id}")

    if cast:
        with ThreadPoolExecutor() as actor_executor:
            for actor_id in cast:
                actor_id, actor_name = get_actor_details(actor_id)
                if actor_name:
                    print(f"  - Actor ID: {actor_id}, Name: {actor_name}")

                    # Insert actor into the database
                    insert_actor_to_database(actor_id, actor_name)

                    # Insert movie-actor relation into the database
                    insert_movie_actor_relation_into_db(movie_id, actor_id)
                else:
                    print(f"  - Actor ID: {actor_id}, Name: Actor details not available")
    else:
        print("No cast information available")

# Function to fetch a page of movies using the discover endpoint
def fetch_movies_page(page_number):
    try:
        discover = tmdb.Discover()
        movies_page = discover.movie(page=page_number)
        return movies_page['results']
    except Exception as e:
        print(f"Failed to fetch movies on page {page_number}: {str(e)}")
        return []

def fetch_large_dataset_of_movies(total_movies=100, movies_per_page=20):
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

# Fetch a large dataset of movies concurrently
total_movies = 100
with ThreadPoolExecutor() as executor:
    movies_to_fetch = fetch_large_dataset_of_movies(total_movies=total_movies)
    executor.map(print_movie_details, movies_to_fetch)

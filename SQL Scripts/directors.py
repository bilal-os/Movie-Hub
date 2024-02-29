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

def insert_director_to_database(name, picture_data, director_id):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Check if picture_data is None
            if picture_data is not None:
                # Insert director's name, ID, and image data into the directors table
                cursor.execute("INSERT INTO directors (id, name, picture) VALUES (?, ?, ?)", director_id, name, pyodbc.Binary(picture_data))
            else:
                # Insert director's name and ID, with picture set to NULL
                cursor.execute("INSERT INTO directors (id, name, picture) VALUES (?, ?, NULL)", director_id, name)

            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Director with ID {director_id} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert director to the database: {str(e)}")


def get_movie_details(movie_id):
    try:
        # Fetch movie details
        movie = tmdb.Movies(movie_id).info()
        title = movie['title']

        # Fetch credits separately to get director information
        credits = tmdb.Movies(movie_id).credits()

        # Assuming the first crew member with the job 'Director' is the movie director
        director = next((crew for crew in credits.get('crew', []) if crew['job'] == 'Director'), None)

        # Return title, director's name, and director's ID
        if director:
            return title, director['name'], director['id']
        else:
            return title, None, None
    except Exception as e:
        print(f"Failed to get movie details: {str(e)}")
        return None, None, None

def get_director_details(director_id):
    try:
        # Fetch director details
        person = tmdb.People(director_id).info()
        director_name = person['name']
        profile_path = person['profile_path']

        # Return director's name and profile image URL
        return director_name, profile_path
    except Exception as e:
        print(f"Failed to get director details: {str(e)}")
        return None, None

def print_movie_details(movie):
    title, director_name, director_id = get_movie_details(movie['id'])
    print(f"Title: {title}, Director: {director_name}", end=' ')

    if director_id:
        director_name, profile_path = get_director_details(director_id)
        if profile_path:
            image_url = f"https://image.tmdb.org/t/p/original{profile_path}"
            print(f"Image URL: {image_url}")

            # Read the image data directly
            picture_data = requests.get(image_url).content

            # Insert director into the database
            insert_director_to_database(director_name, picture_data, director_id)
        else:
            print("Image not available")

            # Insert director into the database with picture set to NULL
            insert_director_to_database(director_name, None, director_id)
    else:
        print("Director details not available")


# Function to fetch a page of movies using the discover endpoint
def fetch_movies_page(page_number):
    try:
        discover = tmdb.Discover()
        movies_page = discover.movie(page=page_number)
        return movies_page['results']
    except Exception as e:
        print(f"Failed to fetch movies on page {page_number}: {str(e)}")
        return []


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

# Fetch a large dataset of movies (e.g., movies_count movies)
large_dataset_of_movies = fetch_large_dataset_of_movies(total_movies=movies_count)

# Print movie details concurrently using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(print_movie_details, large_dataset_of_movies)

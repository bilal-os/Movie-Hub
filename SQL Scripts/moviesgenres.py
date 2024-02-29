import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
import pyodbc

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

def insert_movie_genre_relation_into_db(movie_id, genre_name):
    try:
        # Connect to the database
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()

            # Insert movie-genre relation into the moviegenre table
            cursor.execute("INSERT INTO moviegenre (movie_id, genre_name) VALUES (?, ?)", movie_id, genre_name)
            conn.commit()
    except pyodbc.IntegrityError as e:
        # Handle duplicate key violation
        print(f"Movie-Genre relation for Movie ID {movie_id} and Genre Name {genre_name} already exists in the database.")
    except Exception as e:
        print(f"Failed to insert movie-genre relation into the database: {str(e)}")

def print_movie_genres(movie):
    movie_id = movie['id']
    
    # Fetch movie details to get genre information
    try:
        movie_details = tmdb.Movies(movie_id).info()
        genres = [genre['name'] for genre in movie_details['genres']]
        print(f"Movie ID: {movie_id}, Genres: {genres}")

        # Insert movie-genre relation into the database
        for genre_name in genres:
            insert_movie_genre_relation_into_db(movie_id, genre_name)
    except Exception as e:
        print(f"Failed to fetch genres for Movie ID {movie_id}: {str(e)}")

# Fetch a large dataset of movies concurrently
total_movies = 100
movies_to_fetch = fetch_large_dataset_of_movies(total_movies=total_movies)

# Print and insert movie genres concurrently using ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    executor.map(print_movie_genres, movies_to_fetch)

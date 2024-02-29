import tmdbsimple as tmdb
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import requests
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

def fetch_large_dataset_of_movies(total_movies, movies_per_page=20):
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

def download_image_as_binary(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Failed to download image from {url}: {str(e)}")
        return None

def get_movie_details(movie_id):
    try:
        movie = tmdb.Movies(movie_id).info()
        title = movie['title']
        overview = movie['overview']
        poster_path = movie['poster_path']
        backdrop_path = movie['backdrop_path']
        runtime = movie['runtime']
        rating = movie['vote_average']
        release_date = movie['release_date']

        # Convert runtime from minutes to HH:MM:SS format
        runtime_hours = runtime // 60
        runtime_minutes = runtime % 60
        runtime_formatted = f"{int(runtime_hours):02d}:{int(runtime_minutes):02d}:00"

        # Fetch credits separately to get director information
        credits = tmdb.Movies(movie_id).credits()
        # Assuming the first crew member with the job 'Director' is the movie director
        director = next((crew for crew in credits.get('crew', []) if crew['job'] == 'Director'), None)
        director_id = director['id'] if director else None

        # Fetch trailer information
        trailers = tmdb.Movies(movie_id).videos()
        # Assuming the first trailer in the list is the one to use
        trailer_id = trailers['results'][0]['key'] if trailers['results'] else None

        current_date = datetime.now()
        release_date_datetime = datetime.strptime(release_date, '%Y-%m-%d')
        months_difference = (current_date.year - release_date_datetime.year) * 12 + current_date.month - release_date_datetime.month

        type1 = 'latest' if months_difference <= 5 else 'trending' if rating > 8 else None

        # Construct complete image URLs
        base_image_url = tmdb.Configuration().info()['images']['base_url']
        poster_url = f"{base_image_url}original{poster_path}" if poster_path else None
        backdrop_url = f"{base_image_url}original{backdrop_path}" if backdrop_path else None

        return {
            'id': movie_id,
            'title': title,
            'overview': overview,
            'poster_url': poster_url,
            'backdrop_url': backdrop_url,
            'runtime': runtime_formatted,
            'rating': rating,
            'release_date': release_date,
            'director_id': director_id,
            'trailer_id': trailer_id,
            'type1': type1
        }
    except Exception as e:
        print(f"Failed to get details for movie {movie_id}: {str(e)}")
        return None

def insert_movie_details_into_db(conn, movie_details):
    try:
        # Download poster and backdrop images as binary if URLs are available
        poster_url = movie_details['poster_url']
        backdrop_url = movie_details['backdrop_url']

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
            print(f"Skipping movie {movie_details['id']} due to missing image URL")
            return

        # Insert into the database using parameterized query
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO movies (id, title, overview, poster, background, runtime, rating, release_date, directorid, trailerid, type1)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                movie_details['id'],
                movie_details['title'],
                movie_details['overview'],
                pyodbc.Binary(poster_binary) if poster_binary is not None else None,
                pyodbc.Binary(backdrop_binary) if backdrop_binary is not None else None,
                movie_details['runtime'],
                movie_details['rating'],
                movie_details['release_date'],
                movie_details['director_id'],
                movie_details['trailer_id'],
                movie_details['type1']
            ))
    except pyodbc.Error as e:
        print(f"Failed to insert movie details into the database: {str(e)}")
        print(f"Problematic poster URL: {movie_details['poster_url']}")
        print(f"Problematic backdrop URL: {movie_details['backdrop_url']}")

# Fetch a large dataset of movies concurrently
movies_count = 500
with ThreadPoolExecutor(max_workers=10) as executor:
    movies_to_fetch = fetch_large_dataset_of_movies(total_movies=movies_count)
    futures = [executor.submit(get_movie_details, movie['id']) for movie in movies_to_fetch]

    # Establish a connection to the SQL Server
    try:
        conn = pyodbc.connect(conn_str)

        # Insert movie details into the database concurrently
        with ThreadPoolExecutor(max_workers=10) as insert_executor:
            insert_futures = [insert_executor.submit(insert_movie_details_into_db, pyodbc.connect(conn_str), future.result()) for future in futures]

        # Wait for all insert operations to complete
        for insert_future in insert_futures:
            insert_future.result()

    except Exception as e:
        print(f"Failed to connect to the database or insert data: {str(e)}")

    finally:
        # Close the database connection
        if conn:
            conn.close()

import tmdbsimple as tmdb
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

# Connect to the database
with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()

    # Fetch the list of movie genres
    genres = tmdb.Genres().movie_list()

    # Insert genre names into the genre table
    for genre in genres['genres']:
        genre_name = genre['name']
        try:
            cursor.execute("INSERT INTO genre (genre_name) VALUES (?)", genre_name)
            conn.commit()
            print(f"Genre inserted: {genre_name}")
        except Exception as e:
            print(f"Failed to insert genre {genre_name}: {str(e)}")

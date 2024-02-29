const express = require('express');
const cors = require('cors');
const sql = require('mssql');
const expressAsyncHandler = require('express-async-handler');
const { connectToDatabase, closeDatabaseConnection } = require('./db'); // Update the path accordingly
const nodemailer = require('nodemailer');
const Mailgen = require('mailgen');


const router = express.Router();

// Use the cors middleware
router.use(cors());


const movieController = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();

    // Assuming there's a 'popularity' column to determine trending movies
    const result = await pool.query`
    use moviehub;
      SELECT TOP 5
      movies.id,
    title,
    overview,
    background,
    runtime,
    rating,
    YEAR(release_date) as release_year,
    (
        SELECT STRING_AGG(genre_name, ', ')
        FROM moviegenre mg
        WHERE mg.movie_id = movies.id
    ) as genre_names,
    STRING_AGG(actor.name, ', ') WITHIN GROUP (ORDER BY actorsinMovies.actor_id) as actors_names
FROM
    movies
INNER JOIN
    actorsinMovies on movies.id = actorsinMovies.movie_id
INNER JOIN
    actor on actorsinMovies.actor_id = actor.id
WHERE
    type1 = 'trending'
GROUP BY
    title, overview, background, runtime, rating, release_date, movies.id;
    `;

   const trendingMovies = result.recordset.map(row => {
         // Convert the runtime to a more human-readable format
    const runtimeInMinutes = row.runtime ? Math.floor(row.runtime / 60000) : 0;
    const hours = Math.floor(runtimeInMinutes / 60);
    const minutes = runtimeInMinutes % 60;

      const uniqueActors = [...new Set(row.actors_names.split(', '))]; // Ensure unique actors
        
        const base64Background = Buffer.from(row.background, 'binary').toString('base64');

      return {
        id:row.id,
        title: row.title,
        overview: row.overview,
        background: base64Background,
        runtime: `${hours}h ${minutes}m`,
        rating: row.rating,
        release_year: row.release_year,
        genre_names: row.genre_names.split(', '), // Assuming genre_names is a comma-separated string
        actors_names: uniqueActors.slice(0, 3), // Limiting to the first three unique actors
      };
    });



    res.json(trendingMovies);
  } catch (error) {
    console.error('Error fetching trending movies:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } finally {
    
  }
});

const searchbar = expressAsyncHandler(async (req, res) => {
    let pool;
  try {
    pool = await connectToDatabase();

    // Log a message if the connection is successful
    console.log('Connected to the database');

        const searchTerm = req.body.searchTerm.toLowerCase();


     const result = await sql.query(`
     use moviehub;
      SELECT top 5 id,title, year(release_date) as year1
      FROM movies
      WHERE LOWER(title) LIKE '%${searchTerm}%'
    `);

    // Transform the data to the desired format
    const formattedData = result.recordset.map(row => {
      return {
        id: row.id,
        title: row.title,
        release_year: row.year1,
      };
    });

    // Send the formatted results back to the client
    res.json(formattedData);

  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  } finally {
    
  }

  });


const formatMovieData = (row) => {
  const base64Poster = Buffer.from(row.poster, 'binary').toString('base64');
  const runtimeInMinutes = row.runtime ? Math.floor(row.runtime / 60000) : 0;
  const hours = Math.floor(runtimeInMinutes / 60);
  const minutes = runtimeInMinutes % 60;

  return {
    id: row.id,
    title: row.title,
    overview: row.overview,
    poster: base64Poster,
    runtime: `${hours}h ${minutes}m`,
    rating: row.rating,
    release_year: row.release_year,
    genre_names: row.genre_names ? row.genre_names.split(', ') : [],  };
};

const formatTVSeriesData = (row) => {
  const base64Poster = Buffer.from(row.poster, 'binary').toString('base64');

  return {
    id:row.id,
    title: row.title,
    overview: row.overview,
    poster: base64Poster,
    noOfSeason: row.noOfSeason,
    rating: row.rating,
    release_year: row.release_year,
    genre_names: row.genre_names.split(', '),
  };
};

const justArrivedSection = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();

    // Log a message if the connection is successful
    console.log('Connected to the database');

    // Fetch top 2 latest movies
    const movieResult = await pool.query`
    use moviehub;
      SELECT TOP 2
      movies.id,
        title,
        overview,
        poster,
        runtime,
        rating,
        YEAR(release_date) as release_year,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM moviegenre mg
          WHERE mg.movie_id = movies.id
        ) as genre_names
      FROM
        movies
      WHERE
        type1 = 'latest' and DATEDIFF(DAY, release_date, GETDATE()) <= 7
      GROUP BY
        title, overview, poster, runtime, rating, release_date, movies.id
    `;

    const formattedMovies = movieResult.recordset.map(formatMovieData);

    // Fetch top 2 latest TV series
    const tvSeriesResult = await pool.query`
    use tvhub;
      SELECT TOP 2
      tvseries.id,
        title,
        overview,
        poster,
        noOfSeason,
        rating,
        YEAR(first_air_date) as release_year,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM tvseriesgenre mg
          WHERE mg.tv_series_id = tvseries.id
        ) as genre_names
      FROM
        tvseries
      WHERE
        type1 = 'latest' and DATEDIFF(DAY, GETDATE(), first_air_date) <= 7
      GROUP BY
        title, overview, poster, noOfSeason, rating, first_air_date, tvseries.id
    `;

    const formattedTVSeries = tvSeriesResult.recordset.map(formatTVSeriesData);

    // Combine results before sending the response
    const combinedResults = {
      movies: formattedMovies,
      tvSeries: formattedTVSeries,
    };



    res.json(combinedResults);
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  } finally {
    
  }
});

const popularSection = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();

    // Log a message if the connection is successful
    console.log('Connected to the database');

    // Fetch top 2 latest movies
    const movieResult = await pool.query`
    use moviehub;
         SELECT top 4
    movies.id,
    title,
    overview,
    poster,
    runtime,
    rating,
    YEAR(release_date) as release_year,
    (
        SELECT STRING_AGG(genre_name, ', ')
        FROM moviegenre mg
        WHERE mg.movie_id = movies.id
    ) as genre_names
FROM
    movies
WHERE
    type1 = 'trending' and rating>8
GROUP BY
    title, overview, poster, runtime, rating, release_date, movies.id;
    `;

    const formattedMovies = movieResult.recordset.map(formatMovieData);

    // Fetch top 2 latest TV series
    const tvSeriesResult = await pool.query`
        use tvhub;
      SELECT TOP 4
      tvseries.id,
        title,
        overview,
        poster,
        noOfSeason,
        rating,
        YEAR(first_air_date) as release_year,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM tvseriesgenre mg
          WHERE mg.tv_series_id = tvseries.id
        ) as genre_names
      FROM
        tvseries
      WHERE
        type1 = 'trending' and rating>8
      GROUP BY
        title, overview, poster, noOfSeason, rating, first_air_date, tvseries.id
    `;

    const formattedTVSeries = tvSeriesResult.recordset.map(formatTVSeriesData);

    // Combine results before sending the response
    const combinedResults = {
      movies: formattedMovies,
      tvSeries: formattedTVSeries,
    };


    res.json(combinedResults);
  } catch (error) {
    console.error(error);
    res.status(500).send('Internal Server Error');
  } finally {
    
  }
});

function formatMovieData1(movie) {
  // Slice the actors_names array to contain only the first 3 actors
  const slicedActors = movie.actors_names.split(',').slice(0, 2);
  const base64Poster = Buffer.from(movie.poster, 'binary').toString('base64');
  const runtimeInMinutes = movie.runtime ? Math.floor(movie.runtime / 60000) : 0;
  const hours = Math.floor(runtimeInMinutes / 60);
  const minutes = runtimeInMinutes % 60;

  return {
    id:movie.id,
    title: movie.title,
    overview: movie.overview,
    poster: base64Poster,
    runtime: `${hours}h ${minutes}m`,
    rating: movie.rating,
    director: movie.director_name,
    releaseYear: movie.release_year,
    genreNames: movie.genre_names,
    actorsNames: slicedActors,
  };
}

const bestMovieOfMonth = expressAsyncHandler(async (req, res) => {
  let pool;

  try {
    pool = await connectToDatabase();

    const result = await pool.query`
      use moviehub;
      SELECT TOP 1
        movies.id,
        title,
        overview,
        poster,
        runtime,
        rating,
        directors.name as director_name,
        YEAR(release_date) as release_year,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM moviegenre mg
          WHERE mg.movie_id = movies.id
        ) as genre_names,
        STRING_AGG(actor.name, ', ') WITHIN GROUP (ORDER BY actorsinMovies.actor_id) as actors_names
      FROM
        movies
      INNER JOIN
        actorsinMovies ON movies.id = actorsinMovies.movie_id
      INNER JOIN
        actor ON actorsinMovies.actor_id = actor.id
      INNER JOIN
        directors ON movies.directorid = directors.id
      WHERE
        type1 = 'latest' AND MONTH(release_date) = 10
      GROUP BY
        title, overview, poster, runtime, rating, release_date, movies.id, directors.name
    `;

    if (result.recordset.length > 0) {
      const movie = formatMovieData1(result.recordset[0]);
      res.json(movie);
    } else {
      res.status(404).json({ message: 'No movie found.' });
    }
  } catch (error) {
    console.error('Error fetching latest movie:', error);
    res.status(500).send('Internal Server Error');
  } finally {
    
  }
});

const popularMoviesController = expressAsyncHandler(async (req, res) => {
    let pool;
    try {
       pool = await connectToDatabase();
  
  
      // Assuming there's a 'popularity' column to determine popular movies
      const result = await pool.query`
 use moviehub;
        SELECT top 8
        movies.id,
          title,
          overview,
          background,
          rating,
          YEAR(release_date) as release_year,
          (
            SELECT STRING_AGG(genre_name, ', ')
            FROM moviegenre mg
            WHERE mg.movie_id = movies.id
          ) as genre_names
        FROM
          movies
        INNER JOIN
          moviegenre mg on movies.id = mg.movie_id
        WHERE
          type1 = 'latest'
        GROUP BY
          title, overview, rating, release_date, movies.id,background;
      `;
  
      const popularMovies = result.recordset.map(row => {
        // Convert the rating to a 5-star system
        const ratingInStars = Math.round(row.rating * 5 / 10);
        const base64Background = Buffer.from(row.background, 'binary').toString('base64');
        return {
          id:row.id,
          title: row.title,
          overview: row.overview,
          background: base64Background,
          rating: ratingInStars, // Convert rating to a 5-star system
          release_year: row.release_year,
          genre_names: row.genre_names.split(', '), // Assuming genre_names is a comma-separated string
        };
      });
  
      console.log('Fetched popular movies:', popularMovies);
  
      res.json(popularMovies);
    } catch (error) {
      console.error('Error fetching popular movies:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    } finally {
     
    }
  });
  
  const latestTVSeriesController = expressAsyncHandler(async (req, res) => {
    let pool;
    try {
        pool = await connectToDatabase();

        const result = await pool.query`
		  use tvhub;
      SELECT top 8
      tvseries.id,
    title,
    overview,
    background,
    rating,
    YEAR(first_air_date) as release_year,
    (
      SELECT STRING_AGG(genre_name, ', ')
      FROM tvseriesgenre mg
      WHERE mg.tv_series_id = tvseries.id
    ) as genre_names
  FROM
    tvseries
  INNER JOIN
    tvseriesgenre mg on tvseries.id = mg.tv_series_id
  WHERE
    type1 = 'trending'
  GROUP BY
    title, overview, rating, first_air_date, tvseries.id,background;
        `;

        const trendingTVSeries = result.recordset.map(row => {
            // Convert the rating to a 5-star system
            const ratingInStars = Math.round(row.rating * 5 / 10);
            const base64Background = Buffer.from(row.background, 'binary').toString('base64');
            return {
              id:row.id,
                title: row.title,
                overview: row.overview,
                background: base64Background,
                rating: ratingInStars, // Convert rating to a 5-star system
                release_year: row.release_year,
                genre_names: row.genre_names.split(', '), // Assuming genre_names is a comma-separated string
            };
        });

        console.log('Fetched trending TV series:', trendingTVSeries);

        res.json(trendingTVSeries);
    } catch (error) {
        console.error('Error fetching trending TV series:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    } finally {
        // Close the connection
    }
});

const formatMovieData2 = (row) => {
  const base64Poster = Buffer.from(row.poster, 'binary').toString('base64');
  const base64PosterBack = Buffer.from(row.background, 'binary').toString('base64');
  const runtimeInMinutes = row.runtime ? Math.floor(row.runtime / 60000) : 0;
  const hours = Math.floor(runtimeInMinutes / 60);
  const minutes = runtimeInMinutes % 60;
  const releaseDate = row.release_date ? new Date(row.release_date).toLocaleDateString() : '';


  return {
    title: row.title,
    overview: row.overview,
    poster: base64Poster,
    background: base64PosterBack,
    runtime: `${hours}h ${minutes}m`,
    release_date: releaseDate,
    trailerid: row.trailerid,
        genre_names: row.genre_names ? row.genre_names.split(', ') : [],

  };
};

const getMovieDetails1 = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const movieId = req.body.movieId;

    console.log('Fetching movie details for movieId:', movieId);

    // Use parameterized query to prevent SQL injection
    const result = await pool.query(`
    use moviehub;
      SELECT
        title,
        poster,
        background,
        runtime,
        release_date,
        overview,
        trailerid,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM moviegenre mg
          WHERE mg.movie_id = movies.id
        ) as genre_names
      FROM
        movies
        where id=${movieId}`,
      
    );

    console.log('Fetched movie details:', result);

      // Format the movie details using the provided function
      const movieDetails = result.recordset.map(formatMovieData2);

      // Send the formatted results back to the client
      res.status(200).json(movieDetails);
    
  } catch (error) {
    console.error('Error in getMovieDetails1:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  } 
});

const getMovieCast = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const movieId = req.body.movieId;
    console.log('Fetching movie cast for movieId:', movieId);

    const result = await pool.query(`
      USE moviehub;
    SELECT actor.name, actor.picture
FROM movies
INNER JOIN actorsinmovies ON movies.id = actorsinmovies.movie_id
INNER JOIN actor ON actorsinmovies.actor_id = actor.id
WHERE movies.id = ${movieId};

    `);



    // Format the cast details including converting images to base64
    const castDetails = result.recordset.map((row) => ({
      actorName: row.name,
      actorPicture: Buffer.from(row.picture, 'binary').toString('base64')
    }));

    // Send the formatted results back to the client
    res.status(200).json(castDetails);

  } catch (error) {
    console.error('Error in getMovieCast:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const getMovieDirector = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const movieId = req.body.movieId;
    console.log('Fetching movie cast for movieId:', movieId);

    const result = await pool.query(`
      USE moviehub;
select directors.name, directors.picture from movies
inner join directors on movies.directorid = directors.id
where movies.id = ${movieId}

    `);

    // Format the cast details including converting images to base64
    const directorDetails = result.recordset.map((row) => ({
      directorName: row.name,
      directorPicture: Buffer.from(row.picture, 'binary').toString('base64')
    }));

    // Send the formatted results back to the client
    res.status(200).json(directorDetails);

  } catch (error) {
    console.error('Error in getMovieCast:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const getSimilarMovies = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const movieId = req.body.movieId;
    console.log('Fetching similar movies for movieId:', movieId);

    const result = await pool.query(`
      WITH TargetMovieGenres AS (
          SELECT genre_name
          FROM moviegenre
          WHERE movie_id = ${movieId}
      )

      SELECT TOP 2
          m.id,
          m.title,
          m.overview,
          m.poster,
          m.runtime,
          m.rating,
          YEAR(m.release_date) AS release_year,
          (
              SELECT STRING_AGG(genre_name, ', ')
              FROM moviegenre mg
              WHERE mg.movie_id = m.id
          ) AS genre_names
      FROM
          movies m
      JOIN
          moviegenre mg ON m.id = mg.movie_id
      JOIN
          TargetMovieGenres tg ON mg.genre_name = tg.genre_name
      WHERE
          m.id <> ${movieId}
      GROUP BY
          m.id, m.title, m.overview, m.poster, m.runtime, m.rating, m.release_date
      ORDER BY
          COUNT(DISTINCT mg.genre_name) DESC, m.release_date DESC;
    `);

    const similarMovies = result.recordset.map((row) => {
  const runtimeInMinutes = row.runtime ? Math.floor(row.runtime / 60000) : 0;
  const hours = Math.floor(runtimeInMinutes / 60);
  const minutes = runtimeInMinutes % 60;

  return {
    id: row.id,
    title: row.title,
    overview: row.overview,
    poster: Buffer.from(row.poster, 'binary').toString('base64'),
    runtime: `${hours}h ${minutes}m`,
    rating: row.rating,
    release_year: row.release_year,
    genre_names: row.genre_names.split(', ')
  };
});


    // Send the formatted results back to the client
    res.status(200).json(similarMovies);

  } catch (error) {
    console.error('Error in getSimilarMovies:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const postComment = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();
    const comment = req.body.comment;
    const movieId = req.body.movieId;
    const userName = req.body.userName;
    const currentDate = new Date().toISOString().split('T')[0]; // Get the current date
    console.log('Fetching comment for movieId:', movieId);
    console.log('Fetching comment for userName:', userName);
    console.log('Fetching comment for comment:', comment);
    console.log('Fetching comment for currentDate:', currentDate);
    const result = await pool.query(`
    use moviehub;
      INSERT INTO comments (name, movie_id, Descriptions, date_)
      VALUES ('${userName}', ${movieId}, '${comment}', '${currentDate}');
    `);
    res.status(200).json({ message: 'Comment added successfully' });
  } catch (error) {
    console.error('Error in postComment:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const fetchComments = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();
    const movieId = req.body.movieId;
    console.log('Fetching comments for movieId:', movieId);
    const result = await pool.query(`
      use moviehub;
      SELECT name,date_,Descriptions FROM comments WHERE movie_id = ${movieId};
    `);
    const comments = formatCommentsData(result.recordset);
    res.status(200).json({ comments });
  } catch (error) {
    console.error('Error in fetchComments:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const formatCommentsData = (comments) => {
  return comments.map((comment) => {
    return {
      name: comment.name,
      date: comment.date_.toLocaleDateString(),
      comment: comment.Descriptions
    };
  });
};


const getTvDetails1 = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const tvId = req.body.tvId;

    console.log('Fetching movie details for movieId:', tvId);

    // Use parameterized query to prevent SQL injection
    const result = await pool.query(`
    use tvhub;
      SELECT
        title,
        poster,
        background,
        noOfSeason,
        first_air_date,
        overview,
        trailerid,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM tvseriesgenre mg
          WHERE mg.tv_series_id = tvseries.id
        ) as genre_names
      FROM
        tvseries
        where id=${tvId}`,
      
    );

    console.log('Fetched tv details:', result);

    // Format the movie details using the provided function
    const movieDetails = formatTvSeriesData(result.recordset);

    // Send the formatted results back to the client
    res.status(200).json(movieDetails);
    
  } catch (error) {
    console.error('Error in getMovieDetails1:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  } 
});

const formatTvSeriesData = (row) => {
  return row.map((tvSeries) => {
    // Format the date portion without the time
    const formattedDate = new Date(tvSeries.first_air_date).toLocaleDateString();

    return {
      title: tvSeries.title,
      poster: Buffer.from(tvSeries.poster).toString('base64'),
      background: Buffer.from(tvSeries.background).toString('base64'),
      noOfSeason: tvSeries.noOfSeason,
      first_air_date: formattedDate, // Use the formatted date
      overview: tvSeries.overview,
      trailerid: tvSeries.trailerid,
      genre_names: tvSeries.genre_names,
    };
  });
};

const getTvCast = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const tvId = req.body.tvId;
    console.log('Fetching movie cast for movieId:', tvId);

    const result = await pool.query(`
             USE tvhub;
            SELECT actor.name, actor.picture
            FROM tvseries
            INNER JOIN actorsintvseries ON tvseries.id = actorsintvseries.tv_series_id
            INNER JOIN actor ON actorsintvseries.actor_id = actor.id
            WHERE tvseries.id = ${tvId};

    `);

    // Format the cast details including converting images to base64
    const castDetails = result.recordset.map((row) => ({
      actorName: row.name,
      actorPicture: Buffer.from(row.picture, 'binary').toString('base64')
    }));

    // Send the formatted results back to the client
    res.status(200).json(castDetails);

  } catch (error) {
    console.error('Error in getMovieCast:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const getSimilarTVSeries = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract tvId from the request body
    const tvId = req.body.tvId;
    console.log('Fetching similar TV series for tvId:', tvId);

    const result = await pool.query(`
    use tvhub;
     WITH TargetTVSeriesGenres AS (
  SELECT genre_name
  FROM tvseriesgenre
  WHERE tv_series_id = ${tvId}
)

SELECT TOP 2
  t.id,
  t.title,
  t.overview,
  t.poster,
  t.noOfSeason,
  t.rating,
  t.first_air_date,
  YEAR(t.first_air_date) AS release_year,
  (
    SELECT STRING_AGG(genre_name, ', ')
    FROM tvseriesgenre tg
    WHERE tg.tv_series_id = t.id
  ) AS genre_names
FROM
  tvseries t
JOIN
  tvseriesgenre tg ON t.id = tg.tv_series_id
JOIN
  TargetTVSeriesGenres ttg ON tg.genre_name = ttg.genre_name
WHERE
  t.id <> ${tvId}
GROUP BY
  t.id, t.title, t.overview, t.poster, t.noOfSeason, t.rating, t.first_air_date
ORDER BY
  COUNT(DISTINCT tg.genre_name) DESC, t.first_air_date DESC;
    `);

    const similarTVSeries = result.recordset.map((row) => {
      const noOfSeasons = row.noOfSeason ? row.noOfSeason : 0;

      return {
        id:row.id,
        title: row.title,
        overview: row.overview,
        poster: Buffer.from(row.poster, 'binary').toString('base64'),
        noOfSeasons: noOfSeasons,
        rating: row.rating,
        release_year: row.release_year,
        genre_names: row.genre_names.split(', ')
      };
    });

    // Send the formatted results back to the client
    res.status(200).json(similarTVSeries);

  } catch (error) {
    console.error('Error in getSimilarTVSeries:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const getTvDirector = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    // Connect to the database
    pool = await connectToDatabase();

    // Extract movieId from the request body
    const tvId = req.body.tvId;
    console.log('Fetching movie cast for movieId:', tvId);

    const result = await pool.query(`
      USE tvhub;
      SELECT TOP 1 creators.name, creators.picture FROM tvseries
      INNER JOIN creators ON tvseries.creatorid = creators.id
      WHERE tvseries.id = ${tvId}
    `);

    // Format the cast details including converting images to base64
    const directorDetails = result.recordset.map((row) => ({
      directorName: row.name,
      directorPicture: row.picture ? Buffer.from(row.picture, 'binary').toString('base64') : null
    }));

    // Send the formatted results back to the client
    res.status(200).json(directorDetails);

  } catch (error) {
    console.error('Error in getMovieCast:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const postCommentTv = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();
    const comment = req.body.comment;
    const tvId = req.body.tvId;
    const userName = req.body.userName;
    const currentDate = new Date().toISOString().split('T')[0]; // Get the current date
    console.log('Fetching comment for movieId:', tvId);
    console.log('Fetching comment for userName:', userName);
    console.log('Fetching comment for comment:', comment);
    console.log('Fetching comment for currentDate:', currentDate);
    const result = await pool.query(`
    use tvhub;
      INSERT INTO comments (name, tv_id, Descriptions, date_)
      VALUES ('${userName}', ${tvId}, '${comment}', '${currentDate}');
    `);
    res.status(200).json({ message: 'Comment added successfully' });
  } catch (error) {
    console.error('Error in postComment:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const fetchCommentsTv = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();
    const tvId = req.body.tvId;
    console.log('Fetching comments for movieId:', tvId);
    const result = await pool.query(`
      use tvhub;
      SELECT name,date_,Descriptions FROM comments WHERE tv_id = ${tvId};
    `);
    const comments = formatCommentsData(result.recordset);
    res.status(200).json({ comments });
  } catch (error) {
    console.error('Error in fetchComments:', error);
    // Send a 500 Internal Server Error response
    res.status(500).json({ message: 'Internal server error' });
  }
});

const browseSeriesController=expressAsyncHandler(async (req, res) => {
  let pool;
    try {
        pool = await connectToDatabase();
  
      // Assuming there's a 'popularity' column to determine popular movies
      const result = await pool.query`
      use tvhub
      SELECT
      tvseries.id,
      title,
      overview,
      background,
      rating,
      YEAR(first_air_date) as release_year,
      (
        SELECT STRING_AGG(genre_name, ', ')
        FROM tvseriesgenre mg
        WHERE mg.tv_series_id = tvseries.id
      ) as genre_names
    FROM
      tvseries
    INNER JOIN
      tvseriesgenre mg on tvseries.id = mg.tv_series_id
    GROUP BY
      title, overview, rating, first_air_date, tvseries.id,background;

      `;
  
      const populartvseries = result.recordset.map(row => {
        // Convert the rating to a 5-star system
        const ratingInStars = Math.round(row.rating * 5 / 10);
        const base64Background = Buffer.from(row.background, 'binary').toString('base64');
        return {
          id: row.id,
          title: row.title,
          overview: row.overview,
          background: base64Background,
          rating: ratingInStars, // Convert rating to a 5-star system
          release_year: row.release_year,
          genre_names: row.genre_names.split(', '), // Assuming genre_names is a comma-separated string
        };
      });
  
      console.log('Fetched popular movies:', populartvseries);
  
      res.json(populartvseries);
    } catch (error) {
      console.error('Error fetching popular movies:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    } 
});

const browsemovieController = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();

const { year, rating, genre, searchText } = req.body;

  console.log('searchText', searchText);
  console.log('yearFilter', year);
  console.log('ratingFilter', rating);
  console.log('genreFilter', genre);

    const conditions = [];

    if (year!==undefined) conditions.push(`year(release_date) = ${year}`);
    if (rating!==undefined) conditions.push(`rating >= ${rating}`);
    if (genre!==undefined) conditions.push(`genre_name = '${genre}'`);
    if (searchText!==undefined) conditions.push(`title LIKE '%${searchText}%'`);

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await pool.request()
      .query(`
        use moviehub;
        SELECT distinct movies.id,title,overview,background,rating,YEAR(release_date) as release_year,(SELECT STRING_AGG(genre_name, ', ') FROM moviegenre mg WHERE mg.movie_id = movies.id) as genre_names
        FROM movies
        inner join moviegenre on movies.id=moviegenre.movie_id
        ${whereClause}
      `);

    const movies = result.recordset.map(row => {
      const ratingInStars = Math.round(row.rating * 5 / 10);
      const base64Background = Buffer.from(row.background, 'binary').toString('base64');
      return {
        id: row.id,
        title: row.title,
        overview: row.overview,
        background: base64Background,
        rating: ratingInStars,
        release_year: row.release_year,
        genre_names: row.genre_names.split(', '),
      };
    });


    res.json(movies);
  } catch (error) {
    console.error('Error fetching movies with dynamic criteria:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } 
});

const browseTvController = expressAsyncHandler(async (req, res) => {
  let pool;
  try {
    pool = await connectToDatabase();

const { year, rating, genre, searchText } = req.body;

  console.log('searchText', searchText);
  console.log('yearFilter', year);
  console.log('ratingFilter', rating);
  console.log('genreFilter', genre);

    const conditions = [];

    if (year!==undefined) conditions.push(`year(first_air_date) = ${year}`);
    if (rating!==undefined) conditions.push(`rating >= ${rating}`);
    if (genre!==undefined) conditions.push(`genre_name = '${genre}'`);
    if (searchText!==undefined) conditions.push(`title LIKE '%${searchText}%'`);

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    const result = await pool.request()
      .query(`
        use tvhub;
        SELECT distinct tvseries.id,title,overview,background,rating,YEAR(first_air_date) as release_year,(SELECT STRING_AGG(genre_name, ', ') FROM tvseriesgenre mg WHERE mg.tv_series_id = tvseries.id) as genre_names
        FROM tvseries
        inner join tvseriesgenre on tvseries.id=tvseriesgenre.tv_series_id
        ${whereClause}
      `);

    const movies = result.recordset.map(row => {
      const ratingInStars = Math.round(row.rating * 5 / 10);
      const base64Background = Buffer.from(row.background, 'binary').toString('base64');
      return {
        id: row.id,
        title: row.title,
        overview: row.overview,
        background: base64Background,
        rating: ratingInStars,
        release_year: row.release_year,
        genre_names: row.genre_names.split(', '),
      };
    });


    res.json(movies);
  } catch (error) {
    console.error('Error fetching movies with dynamic criteria:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  } 
});


const sendmail = expressAsyncHandler(async (req,res) => {
    const { name,email,message} = req.body;

    console.log(name,email,message);

    let config = {
        service : 'gmail',
        auth : {
            user: "mb561366@gmail.com",
            pass: "jmeg fcvk xrwj thby"
        }
    }

    let transporter = nodemailer.createTransport(config);

    let MailGenerator = new Mailgen({
        theme: "default",
        product : {
            name: "Mailgen",
            link : 'https://mailgen.js/'
        }
    })

    let response = {
        body: {
            name : "Movie Hub: Contact",
            intro: "Some one has contacted you.",
            table : {
                data : [
                    {
                        Name : name,
                        Email: email,
                        Message: message
                    }
                ]
            },
            outro: "Looking forward to do more business"
        }
    }

    let mail = MailGenerator.generate(response)

    let message1 = {
        from: "MailGen",
        to : "mb561366@gmail.com",
        subject: "Movie Hub Contact",
        html: mail
    }

    transporter.sendMail(message1).then(() => {
        return res.status(201).json({
            msg: "Email Sent Successfully!"
        })
    }).catch(error => {
        return res.status(500).json({ error })
    })


} )

module.exports = {
  movieController,
  searchbar,
  justArrivedSection,
  popularSection,
  bestMovieOfMonth,
  popularMoviesController,
  latestTVSeriesController,
  getMovieDetails1,
  getMovieCast,
  getMovieDirector,
  getSimilarMovies,
  postComment,
  fetchComments,
  getTvDetails1,
  getTvCast,
  getSimilarTVSeries,
  getTvDirector,
  postCommentTv,
  fetchCommentsTv,
  browseSeriesController,
  browsemovieController,
  browseTvController,
  sendmail

};

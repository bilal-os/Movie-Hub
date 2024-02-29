Create database moviehub
use moviehub


drop table movies

TRUNCATE table movies

create table movies
(
id int primary key,
title varchar(250),
overview varchar(1000),
poster varbinary(MAX),
background varbinary(MAX),
runtime time,
rating float,
release_date date,
directorid bigint foreign key references directors(id),
trailerid VARCHAR(500),
type1 varchar(250)
)


    SELECT top 4
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


         SELECT top 5 title, year(release_date) as YEAR1
      FROM movies
      WHERE LOWER(title) LIKE '%opp%'



create table genre
(
genre_name varchar(250) PRIMARY KEY
)

select * from moviegenre

create table directors
(
id bigint  primary key,
name varchar(250),
picture varbinary(MAX)
)


create table moviegenre
(
movie_id int foreign key references movies(id),
genre_name VARCHAR(250) foreign key references genre(genre_name)
primary key(movie_id,genre_name)
)

create table comments
(
comment_id int IDENTITY(1,1) primary key,
name varchar(250),
movie_id int foreign key references movies(id),
Descriptions varchar(1000),
date_ date
)

use moviehub
select * from comments;
SELECT name,date_,Descriptions FROM comments


select comments.name,comments.Descriptions,comments.date_ from comments
inner join movies
on comments.movie_id=movies.id;


create table actor
(
id bigint primary key,
name varchar(250),
picture varbinary(max)
)

create table actorsinMovies
(
movie_id int foreign key references movies(id),
actor_id bigint foreign key references actor(id),
primary key(movie_id,actor_id)
)

drop table movies
drop table directors
drop table actor
drop table actorsinMovies
drop table moviegenre

select * from directors
SELECT * from actor
select * from movies
select * from actorsinMovies
select * from moviegenre
select * from genre

select count(*) from directors
select count(*) from actor
select count(*) from movies
select count(*) from actorsinMovies

SELECT TOP 5 *
FROM movies
WHERE type1 = 'trending';


truncate table movies
truncate table directors
TRUNCATE table actor
truncate table actorsinmovies
TRUNCATE table moviegenre

  use moviehub;
      SELECT TOP 2
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

            use tvhub;
      SELECT TOP 2
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


        use moviehub;
         SELECT top 4
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


               use tvhub;
      SELECT TOP 4
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
select count(*) from movies where type1='latest'

 use moviehub;
        SELECT top 8
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





SELECT
      title,
      poster,
      background,
      runtime,
      release_date,
      (
        SELECT STRING_AGG(genre_name, ', ')
        FROM moviegenre mg
        WHERE mg.movie_id = movies.id
      ) as genre_names
    FROM
      movies
    WHERE
      movies.id = 872585
    GROUP BY
      title, overview, poster, runtime, rating, release_date, background, movies.id;

    use moviehub;
      SELECT
        title,
        overview,
        poster,
        background,
        runtime,
        release_date,
        (
          SELECT STRING_AGG(genre_name, ', ')
          FROM moviegenre mg
          WHERE mg.movie_id = movies.id
        ) as genre_names
      FROM
        movies
      WHERE
        movies.id = 872585

USE moviehub;

SELECT actor.name, actor.picture
FROM movies
INNER JOIN actorsinmovies ON movies.id = actorsinmovies.movie_id
INNER JOIN actor ON actorsinmovies.actor_id = actor.id
WHERE movies.id = 872585;

select directors.name, directors.picture from movies
inner join directors on movies.directorid = directors.id
where movies.id = 872585

-- Select genres of the target movie
WITH TargetMovieGenres AS (
    SELECT genre_id
    FROM moviegenre
    WHERE movie_id = 872585
)

-- Select similar movies based on shared genres
SELECT TOP 2
    m.title,
    m.poster,
    m.rating,
    m.runtime,
    m.overview,
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
    TargetMovieGenres tg ON mg.genre_id = tg.genre_id
WHERE
    m.id <> 872585
GROUP BY
    m.id, m.title, m.poster, m.rating, m.runtime, m.overview, m.release_date
ORDER BY
    COUNT(DISTINCT mg.genre_id) DESC, m.release_date DESC;
select * from movies

USE moviehub;

SELECT *
FROM movies
INNER JOIN moviegenre ON movies.id = moviegenre.movie_id
WHERE 
    (year(release_date) = 2023 OR 2023 IS NULL) -- Year filter, set to 2023 if provided, otherwise ignore
    AND (rating > 4 OR 4 IS NULL) -- Rating filter, set to 8 if provided, otherwise ignore
    AND (genre_name = 'Action' OR 'Action' IS NULL) -- Genre filter, set to 'Action' if provided, otherwise ignore
    AND (movies.title LIKE '%The%' OR '%The%' IS NULL); -- Search term filter, set to the search term if provided, otherwise ignore

 use moviehub;
        SELECT distinct movies.id,title,overview,background,rating,YEAR(release_date) as release_year,(SELECT STRING_AGG(genre_name, ', ') FROM moviegenre mg WHERE mg.movie_id = movies.id) as genre_names
        FROM movies
        inner join moviegenre on movies.id=moviegenre.movie_id
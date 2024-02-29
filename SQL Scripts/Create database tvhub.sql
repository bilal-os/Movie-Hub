Create database tvhub
use tvhub


drop table tvseries

TRUNCATE table tvseries

create table tvseries
(
id int primary key,
title varchar(250),
overview varchar(2000),
poster varbinary(MAX),
background varbinary(MAX),
noOfSeason int,
rating float,
first_air_date date,
creatorid bigint foreign key references creators(id),
trailerid VARCHAR(500),
type1 varchar(250)
)

use tvhub;
        SELECT distinct tvseries.id,title,overview,background,rating,YEAR(first_air_date) as release_year,(SELECT STRING_AGG(genre_name, ', ') FROM tvseriesgenre mg WHERE mg.tv_series_id = tvseries.id) as genre_names
        FROM tvseries
        inner join tvseriesgenre on tvseries.id=tvseriesgenre.tv_series_id

create table genre
(
genre_name varchar(250) PRIMARY KEY
)


USE tvhub;
select top 1 creators.name, creators.picture from tvseries
inner join creators on tvseries.creatorid = creators.id
where tvseries.id = 36

create table creators
(
id bigint  primary key,
name varchar(250),
picture varbinary(MAX)
)


create table tvseriesgenre
(
tv_series_id int foreign key references tvseries(id),
genre_name VARCHAR(250) foreign key references genre(genre_name)
primary key(tv_series_id,genre_name)
)

SELECT top 8
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
    title, overview, poster, noOfSeason, rating, first_air_date, tvseries.id;

use tvhub
create table comments
(
comment_id int IDENTITY(1,1) primary key,
name varchar(250),
tv_id int foreign key references tvseries(id),
Descriptions varchar(1000),
date_ date
)


create table actor
(
id bigint primary key,
name varchar(250),
picture varbinary(max)
)


create table actorsintvseries
(
tv_series_id int foreign key references tvseries(id),
actor_id bigint foreign key references actor(id),
primary key(tv_series_id,actor_id)
)

drop table tvseries
drop table creators
drop table actor
drop table actorsintvseries
drop table tvseriesgenre
drop table genre

select * from directors
SELECT * from actor
select * from tvseries
select * from actorsintvseries
select * from moviegenre
select * from genre

select count(*) from creators
select count(*) from actor
select count(*) from tvseries
select count(*) from actorsintvseries
select count(*) from genre
SELECT count(*) from tvseriesgenre


truncate table tvseries
truncate table creators
TRUNCATE table actor
truncate table actorsintvseries


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
        where id=36


            USE tvhub;
            SELECT actor.name, actor.picture
            FROM tvseries
            INNER JOIN actorsintvseries ON tvseries.id = actorsintvseries.tv_series_id
            INNER JOIN actor ON actorsintvseries.actor_id = actor.id
            WHERE tvseries.id = 36;



WITH TargetTVSeriesGenres AS (
  SELECT genre_name
  FROM tvseriesgenre
  WHERE tv_series_id = 36
)

SELECT TOP 2
  t.title,
  t.overview,
  t.poster,
  t.noOfSeason,
  t.rating,
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
  t.id <> 36
GROUP BY
  t.id, t.title, t.overview, t.poster, t.noOfSeason, t.rating, t.first_air_date
ORDER BY
  COUNT(DISTINCT tg.genre_name) DESC, t.first_air_date DESC;


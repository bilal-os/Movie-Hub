// routes/movieRoutes.js
const express = require('express');
const router = express.Router();
const { movieController,searchbar,justArrivedSection,popularSection,bestMovieOfMonth,latestTVSeriesController,popularMoviesController,getMovieDetails1,getMovieCast,getMovieDirector,getSimilarMovies,postComment,fetchComments,getTvDetails1,getTvCast,getSimilarTVSeries,getTvDirector,postCommentTv,fetchCommentsTv,browseSeriesController,browsemovieController,browseTvController,sendmail } = require('../Controllers/controllers'); 

// Define routes
router.get('/popularMovies', movieController); 
router.post('/searchbar',searchbar); 
router.get('/justArrivedSection',justArrivedSection);
router.get('/popularSection',popularSection);
router.get('/bestMovieOfMonth',bestMovieOfMonth);
router.get('/latestTVSeriesController',latestTVSeriesController);
router.get('/popularMoviesController',popularMoviesController);
router.post('/getMovieDetails1',getMovieDetails1);
router.post('/getMovieCast',getMovieCast);
router.post('/getMovieDirector',getMovieDirector);
router.post('/getSimilarMovies',getSimilarMovies);
router.post('/postComment',postComment);
router.post('/fetchComments',fetchComments);
router.post('/getTvDetails1',getTvDetails1);
router.post('/getTvCast',getTvCast);
router.post('/getSimilarTVSeries',getSimilarTVSeries);
router.post('/getTvDirector',getTvDirector);
router.post('/postCommentTv',postCommentTv);
router.post('/fetchCommentsTv',fetchCommentsTv);
router.get('/browseSeries',browseSeriesController);
router.post('/browseMovies',browsemovieController);
router.post('/browseTv',browseTvController);
router.post('/sendmail',sendmail);
module.exports = router;


﻿using Microsoft.ML;
using Microsoft.ML.Transforms.Text;

using Nest;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSearch
{
    /// <summary>
    /// Basic class of the program.
    /// </summary>
    public class Program
    {
        static async Task Main(string[] args)
        {
            #region Private Variables

            #region Movies

            // The path of the .csv file.
            const string moviesFilePath = "../../../data/movies.csv";

            // The name of the index.
            const string moviesIndexName = "movies";

            #endregion

            #region Ratings

            // The path of the .csv file.
            const string ratingsFilePath = "../../../data/ratings.csv";

            // The name of the index.
            const string ratingsIndexName = "ratings";

            #endregion

            #endregion

            #region From .csv to class

            // Transforms .csv file to Movies class.
            var csvToMovies = Linker.CSVToMovies(moviesFilePath);

            // Transforms .csv file to Ratings class.
            var csvToRatings = Linker.CSVToRatings(ratingsFilePath);

            #endregion

            #region Calls Elastic Search Client

            // Calls the Elastic Search Client.
            var elasticSearchClient = new ElasticClient();

            #endregion

            #region Execute Once

            // Creates the Elastic Search Client.
            //var movieResponse = await elasticSearchClient.Indices.CreateAsync(moviesIndexName);

            // Creates the Elastic Search Client.
            //var ratingResponse = await elasticSearchClient.Indices.CreateAsync(ratingsIndexName);

            // Indexes the Elastic Search Index.
            //var moviesIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(moviesIndexName).IndexMany(csvToMovies));

            // Indexes the Elastic Search Index.
            //var ratingIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(ratingsIndexName).IndexMany(csvToRatings));

            // Deletes the elastic search Index.
            //var movieDelete = await elasticSearchClient.Indices.DeleteAsync(moviesIndexName);

            // Deletes the elastic search Index.
            //var ratingDelete = await elasticSearchClient.Indices.DeleteAsync(ratingsIndexName);

            #endregion

            #region Search testing

            //// Searches all the data of a specific index.
            //var searchMovieResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.MatchAll()).Index(moviesIndexName).Size(10000));

            //// Searches all the data of a specific index.
            //var searchRatingResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.MatchAll()).Index(ratingsIndexName).Size(10000));

            #endregion

            #region First Question

            // User types name of movie.
            Console.WriteLine("Enter name of Movie: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userMovieName = Console.ReadLine();

            // Searches all the data of a specific index matching a string.
            var searchMovieStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x
                                                                     .Query(y => y
                                                                     .MoreLikeThis(z => z
                                                                     .Fields(a => a
                                                                     .Field(b => b
                                                                     .Title))
                                                                     .Like(c => c
                                                                     .Text(userMovieName))
                                                                     .MinTermFrequency(1)))
                                                                     .Index(moviesIndexName)
                                                                     .Size(10000));

            // If we get a result...
            if (searchMovieStringResponce.Documents.Count != 0)
            {
                // Prints the response of the elastic search with user's keyword input.
                Console.WriteLine(searchMovieStringResponce.Documents.Select(x => x.Title).Aggregate((x, y) => x + "\n" + y) + "\n");
            }
            // Else, the result is null...
            else
            {
                // Prints the message below.
                Console.WriteLine("Search of: - " + userMovieName + " - returned zero results\n");
            }

            #endregion

            #region Second Question

            // User types user ID.
            Console.WriteLine("Enter user ID: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userUserId = Console.ReadLine();

            // Joining the two tables of Elastic Search.
            var searchUserIdStringResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x
                                                                      .Query(y => y
                                                                      .Match(z => z
                                                                      .Field(a => a
                                                                      .userId)
                                                                      .Query(userUserId)))
                                                                      .Index(ratingsIndexName)
                                                                      .Size(10000));

            // Creates a list of integers.
            List<int> moviesIds = new List<int>();

            // Creates a list of integers.
            List<float> ratings = new List<float>();

            // For each one of the responses...
            foreach (var item in searchUserIdStringResponce.Documents)
            {
                // Adds the movie ID to the list.
                moviesIds.Add(item.movieId);

                // Adds the movie ID to the list.
                ratings.Add(item.rating);
            }

            // Creates a list of strings.
            List<string> moviesNames = new List<string>();

            // For each one of the responses...
            foreach (var item in moviesIds)
            {
                // Searches all the data of a specific index matching an item.
                var movieNamesByMovieIds = await elasticSearchClient.SearchAsync<Movies>(x => x
                                                                    .Query(y => y
                                                                    .Match(z => z
                                                                    .Field(a => a
                                                                    .MovieId)
                                                                    .Query(item
                                                                    .ToString())))
                                                                    .Index(moviesIndexName)
                                                                    .Size(10000));

                // TODO: Add only the title of the movies.
                moviesNames.Add(movieNamesByMovieIds.ToString());
            }

            // Creates a list of floats.
            List<float> averageRatingsForAllMoviesOfAUser = new List<float>();

            // For each movie ID...
            foreach (var item in moviesIds)
            {
                // Searches all the ratings of a specific movie ID.
                var ratingsResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x
                                                               .Query(y => y
                                                               .Match(z => z
                                                               .Field(a => a
                                                               .movieId)
                                                               .Query(item
                                                               .ToString())))
                                                               .Index(ratingsIndexName)
                                                               .Size(10000));

                // Creates a list of floats.
                List<float> userRatings = new List<float>();

                // For each rating...
                foreach (var rating in ratingsResponse.Documents)
                {
                    // Adds a rating to allRatings list.
                    userRatings.Add(rating.rating);
                }

                // Gets the average value of all ratings of a movie.
                var averageRatingForAMovieOfAUser = userRatings.Average();

                // Adds the average rating to the list.
                averageRatingsForAllMoviesOfAUser.Add(averageRatingForAMovieOfAUser);

                // Prints average rating for a movie of a user.
                //Console.WriteLine(averageRatingForAMovieOfAUser);

                // Empty allRatings list.
                userRatings.Clear();
            }

            // Creates a list that concatenates the three lists we created before.
            var listOfMoviesRatingsAndAverageRatings = Enumerable.Range(0, moviesIds.Count).Select(i => new
            {
                moviesIds = moviesIds[i],
                ratings = ratings[i],
                averageRatingsForAllMoviesOfAUser = averageRatingsForAllMoviesOfAUser[i],
            });

            // Sorts the list by descending ratings and
            var sortedListOfMoviesRatingsAndAverageRatings = listOfMoviesRatingsAndAverageRatings.OrderByDescending(x => x.ratings).ThenByDescending(z => z.averageRatingsForAllMoviesOfAUser).ToList();


            // For each item in those three lists...
            foreach (var item in sortedListOfMoviesRatingsAndAverageRatings)
            {
                // Prints the item.
                Console.WriteLine(item);
            }

            // Empty line.
            Console.WriteLine();

            #endregion

            #region All Different Users

            // Gets all different users.
            var allDifferentUsersResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x.Source(s => s.Includes(i => i.Field(f => f.userId))).Query(q => q.Match(m => m.Field(a => a.userId))).Collapse(c => c.Field(b => b.userId)).Index(ratingsIndexName).Size(10000));

            // Gets all different users to list.
            var allDifferentUsersListResponse = allDifferentUsersResponse.Documents.Select(x => x.userId).ToList();

            #endregion

            #region All Different Movies

            // Gets all different movies.
            var allDifferentMoviesResponse = await elasticSearchClient.SearchAsync<Movies>(x => x.Source(s => s.Includes(i => i.Field(f => f.MovieId))).Query(q => q.Match(m => m.Field(a => a.MovieId))).Collapse(c => c.Field(b => b.MovieId)).Index(moviesIndexName).Size(10000));

            // Gets all different movies to list.
            var allDifferentMoviesListResponse = allDifferentMoviesResponse.Documents.Select(x => x.MovieId).ToList();

            #endregion

            #region All Different Genres

            // Gets all categories.
            var allGenresResponse = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.Genres))).Index(moviesIndexName).Size(10000));

            // Creates list of strings.
            List<string> allDifferentGenresListResponse = new List<string>();

            // For each one categories response...
            foreach (var item in allGenresResponse.Documents)
            {
                //allDifferentCategoriesResponse.Add(item.movieId);
                // For each genre in genres...
                foreach (var genre in item.Genres)
                {
                    // If there is not already in the list...
                    if (!allDifferentGenresListResponse.Contains(genre))
                    {
                        // Adds genre to the list.
                        allDifferentGenresListResponse.Add(genre);
                    }
                }
            }

            #endregion

            #region Takes average rating per category and per user

            // Creates a dictionary with key as string and value as list of integers.
            var genreAndMovies = new Dictionary<string, List<int>>();

            // For each genre in all different genres List...
            foreach (var genre in allDifferentGenresListResponse)
            {
                // Gets the genres of the movies.
                var genreOfMovies = await elasticSearchClient.SearchAsync<Movies>(s => s
                                                             .Query(q => q
                                                             .Match(m => m
                                                             .Field(f => f
                                                             .Genres)
                                                             .Query(genre)))
                                                             .Index(moviesIndexName)
                                                             .Size(10000));

                // Adds genres and movies to the list.
                genreAndMovies.Add(genre, genreOfMovies.Documents.Select(s => s.MovieId).ToList());
            }

            // Create a dictionary with key as integer and value as dictionary with key as string and value as float.
            var ratingsPerUserAndGenre = new Dictionary<int, Dictionary<string, float>>();

            // For each use in add different users...
            foreach (var user in allDifferentUsersListResponse)
            {
                // Create a dictionary with key as string and value as float.
                var listOfAverageRatings = new Dictionary<string, float>();

                // For each key pair value in genres and movies...
                foreach (KeyValuePair<string, List<int>> entry in genreAndMovies)
                {
                    // Gets the movie rating.
                    var movieRatingPerUserPerCategory = await elasticSearchClient.SearchAsync<Ratings>(x => x
                                                                                 .Source(s => s
                                                                                 .Includes(i => i
                                                                                 .Fields(f => f
                                                                                 .rating)))
                                                                                 .Query(y => y
                                                                                 .Bool(b => b
                                                                                 .Filter(fil => fil
                                                                                 .Terms(m => m
                                                                                 .Field(g => g
                                                                                 .userId)
                                                                                 .Terms(user)), fil => fil
                                                                                 .Terms(m => m
                                                                                 .Field(g => g
                                                                                 .movieId)
                                                                                 .Terms(entry.Value)))))
                                                                                 .Index(ratingsIndexName)
                                                                                 .Size(10000));

                    // Adds the genre to the key of the dictionary and the average of the movie ratings of a genre of a user to the value.
                    listOfAverageRatings.Add(entry.Key, movieRatingPerUserPerCategory.Documents.Count == 0 ? 0 : movieRatingPerUserPerCategory.Documents.Select(s => s.rating).ToList().Average());
                }
                // Adds the user to the key of the dictionary and the list of ratings to the value.
                ratingsPerUserAndGenre.Add(user, listOfAverageRatings);
            }

            #endregion

            #region K-Means

            #region Clustering

            // Creates the machine learning context.
            var mlContext = new MLContext();

            // Creates an IEnumerable of lists of floats.
            var ratingsPerUserAndGenreToClass = ratingsPerUserAndGenre.Select(s => s.Value.Select(x => x.Value).ToList());

            // Creates a list of AverageRatingPerGenre type.
            var dataSet = new List<AverageRatingPerGenre>();

            // For each item in the existing class...
            foreach (var item in ratingsPerUserAndGenreToClass)
            {
                // Adds to the dataset a new object.
                dataSet.Add(new AverageRatingPerGenre(item));
            }

            // Creates the IDataView of the dataset.
            IDataView trainingData = mlContext.Data.LoadFromEnumerable(dataSet);

            // Get the column names
            var propertyNames = typeof(AverageRatingPerGenre).GetProperties().Select(x => x.Name).ToArray();

            // Choose a number of clusters.
            var numberOfClusters = 3;

            #endregion

            #region Training

            // Initialize the k-means trainer
            var kMeansTrainer = mlContext.Transforms.Concatenate("Features", propertyNames)
                                                    .Append(mlContext.Clustering.Trainers
                                                    .KMeans("Features", numberOfClusters: numberOfClusters));

            // Train the model
            var trainedAverageRatingsModel = kMeansTrainer.Fit(trainingData);

            // Run the model on the same data set
            var transformedAverageRatingsData = trainedAverageRatingsModel.Transform(trainingData);

            #endregion

            #region Prediction

            // Get the predictions
            var predictions = mlContext.Data.CreateEnumerable<Prediction>(transformedAverageRatingsData, false).ToList();

            // Creates a dictionary with key an integer and value a dictionary with key an integer and value a dictionary with key an integer and value a float.
            var allClustersAllUsersAndAllMoviesRatings = new Dictionary<int, Dictionary<int, Dictionary<int, float>>>();

            // For each one of the clusters...
            for (int cluster = 1; cluster <= numberOfClusters; cluster++)
            {
                Console.WriteLine("Cluster No. " + cluster);

                // Every respond of the current cluster.
                var respondForCluster = predictions.Where(w => w.PredictedLabel == cluster);

                // Creates list of integers.
                var usersOfACluster = new List<int>();

                // For each item in the respond of the current cluster...
                foreach (var item in respondForCluster)
                {
                    // Takes the index of an item.
                    var index = predictions.IndexOf(item);

                    // Finds the user with the specific index.
                    var indexOfUser = allDifferentUsersListResponse.ElementAt(index);

                    // Gets the user ID by adding 1 to the index we got.
                    var userId = indexOfUser + 1;

                    // Adds the users to the users of the current cluster list.
                    usersOfACluster.Add(userId);
                }

                // Creates a dictionary with key an integer and value a dictionary with key an integer and value a float.
                var allUsersAndAllMoviesRatings = new Dictionary<int, Dictionary<int, float>>();

                // Creates a list of integers.
                var movieIdsAlreadyCalculated = new List<int>();

                // For each user in users of a cluster...
                foreach (var user in usersOfACluster)
                {
                    // Gets all the movies that the user has rated.
                    var moviesOfAUser = await elasticSearchClient.SearchAsync<Ratings>(x => x
                                                                 .Query(y => y
                                                                 .Match(m => m
                                                                 .Field(f => f
                                                                 .userId)
                                                                 .Query(user
                                                                 .ToString())))
                                                                 .Index(ratingsIndexName)
                                                                 .Size(10000));

                    // Creates a list with all the movie IDs of a user.
                    var moviesOfAUserList = moviesOfAUser.Documents.Select(s => s.movieId).ToList();

                    // Creates a dictionary with key an integer and value a float.
                    var allMoviesRatings = new Dictionary<int, float>();

                    // For each movie...
                    foreach (var movie in allDifferentMoviesListResponse)
                    {
                        // If the user has NOT already rated this movie...
                        if (!moviesOfAUserList.Contains(movie) && !movieIdsAlreadyCalculated.Contains(movie))
                        {
                            // Gets the movie rating.
                            var ratingOfOthersOfCluster = await elasticSearchClient.SearchAsync<Ratings>(x => x
                                                                                   .Source(s => s
                                                                                   .Includes(i => i
                                                                                   .Fields(f => f
                                                                                   .rating)))
                                                                                   .Query(y => y
                                                                                   .Bool(b => b
                                                                                   .Filter(fil => fil
                                                                                   .Terms(m => m
                                                                                   .Field(g => g
                                                                                   .userId)
                                                                                   .Terms(usersOfACluster)), fil => fil
                                                                                   .Terms(m => m
                                                                                   .Field(g => g
                                                                                   .movieId)
                                                                                   .Terms(movie)))))
                                                                                   .Index(ratingsIndexName)
                                                                                   .Size(10000));

                            // Adds to the dictionary movie ID as the key and the average of ratings of other users of the same cluster as a value.
                            allMoviesRatings.Add(movie, ratingOfOthersOfCluster.Documents.Count == 0 ? 0 : ratingOfOthersOfCluster.Documents.Select(s => s.rating).Average());

                            // Adds the movie ID to the list of movies that have already been calculated.
                            movieIdsAlreadyCalculated.Add(movie);
                        }
                    }
                    // Adds to the dictionary user ID as the key and the dictionary of average of ratings of other users of the same cluster as a value.
                    allUsersAndAllMoviesRatings.Add(user, allMoviesRatings);
                }
                // Adds to the dictionary cluster as the key and the dictionary of all users, all movies and average ratings as a value.
                allClustersAllUsersAndAllMoviesRatings.Add(cluster, allUsersAndAllMoviesRatings);
            }

            #endregion

            #endregion

            #region Neural network

            #region Word embeddings

            // Gets an empty samples list of ratings.
            var emptySamples = new List<Movies>();

            // For each item in the existing class...
            foreach (var item in allDifferentMoviesListResponse)
            {
                // Adds to the dataset a new object.
                emptySamples.Add(new Movies(item));
            }

            // Convert sample list to an empty IDataView.
            var emptyDataView = mlContext.Data.LoadFromEnumerable(emptySamples);

            var textPipeline = mlContext.Transforms.Text.NormalizeText("Text")
                                                        .Append(mlContext.Transforms.Text.TokenizeIntoWords("Tokens", "Text"))
                                                        .Append(mlContext.Transforms.Text.ApplyWordEmbedding("Features", "Tokens",
                                                        WordEmbeddingEstimator.PretrainedModelKind.SentimentSpecificWordEmbedding));

            // Fit to data.
            var textTransformer = textPipeline.Fit(emptyDataView);

            #endregion

            #region One hot encoding

            #endregion

            #region Prediction

            #endregion

            #endregion
        }
    }
}

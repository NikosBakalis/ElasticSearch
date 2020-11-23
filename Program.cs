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

            // Transforms .csv file to Movies class.
            var csvToMovies = Linker.CSVToMovies(moviesFilePath);

            // Transforms .csv file to Ratings class.
            var csvToRatings = Linker.CSVToRatings(ratingsFilePath);

            // Calls the Elastic Search Client.
            var elasticSearchClient = new ElasticClient();

            #region Execute Once

            // Creates the Elastic Search Client.
            //var movieResponse = await elasticSearchClient.Indices.CreateAsync(moviesIndexName);

            // Creates the Elastic Search Client.
            //var ratingResponse = await elasticSearchClient.Indices.CreateAsync(ratingsIndexName);

            // Deletes the elastic search Index.
            //var movieDelete = await elasticSearchClient.Indices.DeleteAsync(moviesIndexName);

            // Deletes the elastic search Index.
            //var ratingDelete = await elasticSearchClient.Indices.DeleteAsync(ratingsIndexName);

            // Indexes the Elastic Search Index.
            //var moviesIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(moviesIndexName).IndexMany(csvToMovies));

            // Indexes the Elastic Search Index.
            //var ratingIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(ratingsIndexName).IndexMany(csvToRatings));

            #endregion

            // Searches all the data of a specific index.
            //var searchMovieResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.MatchAll()).Index(moviesIndexName).Size(10000));

            // Searches all the data of a specific index.
            //var searchRatingResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.MatchAll()).Index(ratingsIndexName).Size(10000));
            
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
                                                                     .title))
                                                                     .Like(c => c
                                                                     .Text(userMovieName))
                                                                     .MinTermFrequency(1)))
                                                                     .Index(moviesIndexName)
                                                                     .Size(10000));

            // If we get a result...
            if (searchMovieStringResponce.Documents.Count != 0)
            {
                // Prints the response of the elastic search with user's keyword input.
                Console.WriteLine(searchMovieStringResponce.Documents.Select(x => x.title).Aggregate((x, y) => x + "\n" + y));
            }

            // Else, the result is null...
            else
            {
                // Prints the message below.
                Console.WriteLine("Search of: - " + userMovieName + " - returned zero results");
            }

            #endregion

            // Empty line.
            Console.WriteLine();

            #region Second Question

            // User types user ID.
            Console.WriteLine("Enter user ID: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userUserId = Console.ReadLine();

            // Joining the two tables of Elastic Search.
            var searchUserIdStringResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.userId).Query(userUserId))).Index(ratingsIndexName).Size(10000));

            // Creates a list of integers.
            List<float> moviesIds = new List<float>();

            // For each one of the responses...
            foreach (var item in searchUserIdStringResponce.Documents)
            {
                // Adds the movie ID to the list.
                moviesIds.Add(item.movieId);
            }

            // Creates a list of strings.
            List<string> moviesNames = new List<string>();

            // For each one of the responses...
            foreach (var item in moviesIds)
            {
                // Searches all the data of a specific index matching an item.
                var movieNamesByMovieIds = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.movieId).Query(item.ToString()))).Index(moviesIndexName).Size(10000));

                // TODO: Add only the title of the movies.
                moviesNames.Add(movieNamesByMovieIds.ToString());
            }

            // Creates a list of integers.
            List<float> ratings = new List<float>();

            // For each one of the responses...
            foreach (var item in searchUserIdStringResponce.Documents)
            {
                // Adds the movie ID to the list.
                ratings.Add(item.rating);
            }

            // Creates a list of floats.
            List<float> averageRatingsForAllMoviesOfAUser = new List<float>();

            // For each movie ID...
            foreach (var item in moviesIds)
            {
                // Searches all the ratings of a specific movie ID.
                var ratingsResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.movieId).Query(item.ToString()))).Index(ratingsIndexName).Size(10000));

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

            #endregion

            // Empty line.
            Console.WriteLine();

            // Gets all different users.
            var allDifferentUsersResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x.Source(s => s.Includes(i => i.Field(f => f.userId))).Query(q => q.Match(m => m.Field(a => a.userId))).Collapse(c => c.Field(b => b.userId)).Index(ratingsIndexName).Size(10000));

            // Gets all different users to list.
            var allDifferentUsersListResponse = allDifferentUsersResponse.Documents.Select(x => x.userId).ToList();

            // Gets all different movies.
            var allDifferentMoviesResponse = await elasticSearchClient.SearchAsync<Movies>(x => x.Source(s => s.Includes(i => i.Field(f => f.movieId))).Query(q => q.Match(m => m.Field(a => a.movieId))).Collapse(c => c.Field(b => b.movieId)).Index(moviesIndexName).Size(10000));

            // Gets all different movies to list.
            var allDifferentMoviesListResponse = allDifferentMoviesResponse.Documents.Select(x => x.movieId).ToList();

            // Gets all categories.
            var allCategoriesResponse = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.genres))).Index(moviesIndexName).Size(10000));

            // Creates list of strings.
            List<string> allDifferentCategoriesResponse = new List<string>();

            // For each one categories response...
            foreach (var item in allCategoriesResponse.Documents)
            {
                //allDifferentCategoriesResponse.Add(item.movieId);
                // For each genre in genres...
                foreach (var genre in item.genres)
                {
                    // If there is not already in the list...
                    if (!allDifferentCategoriesResponse.Contains(genre))
                    {
                        // Adds genre to the list.
                        allDifferentCategoriesResponse.Add(genre);
                    }
                }
            }

            // Creates dictionary with key and value.
            Dictionary<int, List<string>> userAndHisCategoriesDictionary = new Dictionary<int, List<string>>();

            // Creates dictionary with key and value.
            Dictionary<int, List<int>> userAndHisMoviesDictionary = new Dictionary<int, List<int>>();

            // For each user in the user list...
            foreach (var user in allDifferentUsersListResponse)
            {
                // Creates list of strings.
                List<string> allDifferentCategoriesOfUsersRatings = new List<string>();

                // Gets all movies he has rated.
                var allMoviesOfAUserResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.userId).Query(user.ToString()))).Index(ratingsIndexName).Size(10000));

                // For each of the movies the user has rated...
                foreach (var movie in allMoviesOfAUserResponse.Documents.Select(x => x.movieId).ToList())
                {
                    // Gets the genres of the movie.
                    var allCategoriesOfAMovieOfAUserResponse = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.movieId).Query(movie.ToString()))).Index(moviesIndexName).Size(10000));

                    // For each of the categories in the ...
                    foreach (var category in allCategoriesOfAMovieOfAUserResponse.Documents.Select(x => x.genres).ToList())
                    {
                        // For each of the items in the category...
                        foreach (var item in category)
                        {
                            // If list does not already contains the item...
                            if (!allDifferentCategoriesOfUsersRatings.Contains(item))
                            {
                                // Adds the item to the list.
                                allDifferentCategoriesOfUsersRatings.Add(item);
                            }
                        }
                    }
                }
                // Adds to the dictionary the user as the key and the movie as a value.
                userAndHisMoviesDictionary.Add(user, allMoviesOfAUserResponse.Documents.Select(x => x.movieId).ToList());

                // Adds to the dictionary the user as the key and the category as a value.
                userAndHisCategoriesDictionary.Add(user, allDifferentCategoriesOfUsersRatings);
            }

            // Creates dictionary with key and value.
            Dictionary<string, List<int>> categoryAndItsUsersDictionary = new Dictionary<string, List<int>>();

            // For each category in the different categories list...
            foreach (var category in allDifferentCategoriesResponse)
            {
                // Creates list to store the users of a category.
                List<int> usersOfACategory = new List<int>();

                // For each key and value in dictionary...
                foreach (KeyValuePair<int, List<string>> entry in userAndHisCategoriesDictionary)
                {
                    // For each category to the list of the dictionary...
                    foreach (var categoryToTest in entry.Value)
                    {
                        // If the category is equal to the category...
                        if (category.Equals(categoryToTest))
                        {
                            // Adds the key to the list.
                            usersOfACategory.Add(entry.Key);
                        }
                    }
                }
                // Adds to the dictionary the category as a key and the user as a value.
                categoryAndItsUsersDictionary.Add(category, usersOfACategory);
            }

            // Creates dictionary with key and value.
            Dictionary<string, List<float>> categoryAndItsRatingsDictionary = new Dictionary<string, List<float>>();

            // For each key and value in dictionary...
            foreach (KeyValuePair<string, List<int>> entry in categoryAndItsUsersDictionary)
            {
                // Creates list of float.
                List<float> ratingList = new List<float>();

                // For each user in users dictionary...
                foreach (var user in entry.Value)
                {
                    // Gets all movies he has rated.
                    var movieIdsOfAUser = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.userId).Query(user.ToString()))).Index(ratingsIndexName).Size(10000));

                    // Creates list of floats.
                    List<float> allRatingsOfAUser = new List<float>();
                    
                    // For each movie in movies of a user...
                    foreach (var movieId in movieIdsOfAUser.Documents.Select(x => x.movieId).ToList())
                    {
                        // Gets all genres of a movies.
                        var genresOfAMovie = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.movieId).Query(movieId.ToString()))).Index(moviesIndexName).Size(10000));

                        // For each genres in the genres of movies...
                        foreach (var genres in genresOfAMovie.Documents.Select(x => x.genres).ToList())
                        {
                            // For each genre in the genres list...
                            foreach (var genre in genres)
                            {
                                // If the genre is equal to the genre of the initial dictionary...
                                if (genre.Equals(entry.Key))
                                {
                                    // Gets all ratings of the movies of the user of a specific genre.
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
                                                                                                 .Terms(movieId)))))
                                                                                                 .Collapse(x => x
                                                                                                 .Field(a => a
                                                                                                 .rating))
                                                                                                 .Index(ratingsIndexName)
                                                                                                 .Size(10000));

                                    // Puts a rating to a list.
                                    var rating = movieRatingPerUserPerCategory.Documents.Select(x => x.rating).ToList();

                                    // Puts all ratings to a list.
                                    allRatingsOfAUser.Add(rating[0]);
                                }
                            }
                        }
                    }
                    // Puts the average of all ratings of a user of a specific genre to a list.
                    ratingList.Add(allRatingsOfAUser.Average());
                }
                // Adds to the dictionary the category as a key and the rating as a value.
                categoryAndItsRatingsDictionary.Add(entry.Key, ratingList);
            }
            // Puts the genres from the dictionary to a list.
            var genresList = new List<string>(categoryAndItsUsersDictionary.Keys);

            // Puts the users ids from the dictionary to a list.
            var userIdList = new List<List<int>>(categoryAndItsUsersDictionary.Values);

            // Puts the ratings from the dictionary to a list.
            var ratingsList = new List<List<float>>(categoryAndItsRatingsDictionary.Values);

            // Creates a list that concatenates the three lists we created before.
            var genresUserIdsAndRatingsList = Enumerable.Range(0, moviesIds.Count).Select(i => new
            {
                genresList = genresList[i],
                userIdList = userIdList[i],
                ratingsList = ratingsList[i],
            });
        }
    }
}

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
            var searchMovieResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.MatchAll()).Index(moviesIndexName).Size(10000));

            // Searches all the data of a specific index.
            //var searchRatingResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.MatchAll()).Index(ratingsIndexName).Size(10000));

            // User types name of movie.
            Console.WriteLine("Enter name of Movie: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userMovieName = Console.ReadLine();

            // Searches all the data of a specific index matching a string.
            var searchMovieStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x
                                                                     .Query(y => y
                                                                     .MoreLikeThis(z => z
                                                                     .Fields(a => a
                                                                     .Field(b => b.title))
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
            
            // Empty line.
            Console.WriteLine();

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

            var sortedListOfMoviesRatingsAndAverageRatings = listOfMoviesRatingsAndAverageRatings.OrderByDescending(x => x.ratings).ThenByDescending (z => z.averageRatingsForAllMoviesOfAUser).ToList();


            // For each item in those three lists...
            foreach (var item in sortedListOfMoviesRatingsAndAverageRatings)
            {
                // Prints the item.
                Console.WriteLine(item);
            }

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

            // Gets all ratings.
            var allRatingsResponse = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.rating))).Index(ratingsIndexName).Size(10000));

            foreach (var item in allRatingsResponse.Documents)
            {

            }

            foreach (var item in allDifferentCategoriesResponse)
            {

            }
        }
    }
}

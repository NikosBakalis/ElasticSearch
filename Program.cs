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

            //  Creates the Elastic Search Client.
            //var movieResponse = await elasticSearchClient.Indices.CreateAsync(moviesIndexName);

            //  Creates the Elastic Search Client.
            //var ratingResponse = await elasticSearchClient.Indices.CreateAsync(ratingsIndexName);

            // Deletes the elastic search Index.
            //var movieDelete = await elasticSearchClient.Indices.DeleteAsync(moviesIndexName);

            // Deletes the elastic search Index.
            //var ratingDelete = await elasticSearchClient.Indices.DeleteAsync(ratingsIndexName);

            // Indexes the Elastic Search Index.
            //var moviesIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(moviesIndexName).IndexMany(csvToMovies));

            // Indexes the Elastic Search Index.
            //var ratingIndexResponce = await elasticSearchClient.BulkAsync(x => x.Index(ratingsIndexName).IndexMany(csvToRatings));

            // Searches all the data of a specific index.
            var searchMovieResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.MatchAll()).Index(moviesIndexName).Size(10000));

            // Searches all the data of a specific index.
            var searchRatingResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.MatchAll()).Index(ratingsIndexName).Size(10000));

            // User types name of movie.
            Console.WriteLine("Enter name of Movie: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userMovieName = Console.ReadLine();

            // Searches all the data of a specific index matching a string.
            var searchMovieStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.title).Query(userMovieName))).Index(moviesIndexName).Size(10000));
            //var searchStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.title).Query("(1995)"))).Index("movies").Size(10000));

            // User types user ID.
            Console.WriteLine("Enter user ID: ");

            // Create a string variable and get user input from the keyboard and store it in the variable.
            string userUserId = Console.ReadLine();

            // Joining the two tables of Elastic Search.
            var searchUserIdStringResponce = await elasticSearchClient.SearchAsync<Ratings>(x => x.Query(y => y.Match(z => z.Field(a => a.userId).Query(userUserId))).Index(ratingsIndexName).Size(10000));

            // Creates a list of integers.
            List<int> moviesIds = new List<int>();

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
        }
    }
}

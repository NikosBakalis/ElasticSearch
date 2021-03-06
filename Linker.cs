﻿using Microsoft.ML;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ElasticSearch
{
    /// <summary>
    /// Class for methods and connections.
    /// </summary>
    public static class Linker
    {
        #region Public Methods
        
        /// <summary>
        /// Transforms the .csv file data to movies class data.
        /// </summary>
        /// <param name="csvPath">The path of the movies.csv file</param>
        /// <returns>The list of the movies</returns>
        public static IEnumerable<Movies> CSVToMovies(string csvPath)
        {
            // Gets all lines from the .csv file.
            var movieLines = File.ReadAllLines(csvPath).Select(a => a.Split(',')).Skip(1);

            // Creates a list of movies.
            var movieList = new List<Movies>();

            // For each line in lines of the .csv file...
            foreach (var line in movieLines)
            {
                // If The length of the line is greater than 3...
                if (line.Length > 3)
                {
                    // Adds a movie to the movieList that contains:
                    movieList.Add(new Movies()
                    {
                        // A movie ID,
                        MovieId = int.Parse(line[0]),

                        // A title, (This title may contains "," characters so we treat them real good.)
                        Title = line.Skip(1).Take(line.Length - 2).Aggregate((x, y) => x + "," + y),

                        // And some genres separated with this "|" character.
                        Genres = new List<string>(line[^1].Split("|"))
                    });
                }
                // else...
                else
                {
                    // Adds a movie to the movieList that contains:
                    movieList.Add(new Movies()
                    {
                        // A movie ID,
                        MovieId = int.Parse(line[0]),

                        // A title,
                        Title = line[1],

                        // And some genres separated with this "|" character.
                        Genres = new List<string>(line[2].Split("|"))
                    });
                }
            }
            // Returns the list of movies.
            return movieList;
        }

        /// <summary>
        /// Transforms the .csv file data to ratings class data.
        /// </summary>
        /// <param name="csvPath">The path of the ratings.csv file</param>
        /// <returns>The list of the ratings</returns>
        public static IEnumerable<Ratings> CSVToRatings(string csvPath)
        {
            // Gets all lines from the .csv file.
            var ratingLines = File.ReadAllLines(csvPath).Select(a => a.Split(',')).Skip(1);

            // Creates a list of movies.
            var ratingList = new List<Ratings>();

            // For each line in lines of the .csv file...
            foreach (var line in ratingLines)
            {
                // Adds a movie to the movieList that contains:
                ratingList.Add(new Ratings()
                {
                    // A user ID,
                    userId = int.Parse(line[0]),

                    // A movie ID,
                    movieId = int.Parse(line[1]),

                    // A rating,
                    rating = float.Parse(line[2]),

                    // And a time-stamp.
                    timestamp = line[3]
                });
            }

            return ratingList.Where(x => x.movieId <= 9125).ToList();
        }

        public static (IDataView training, IDataView test) LoadData(MLContext mlContext)
        {
            var trainingDataPath = Path.Combine(Environment.CurrentDirectory, "Data", "recommendation-ratings-train.csv");
            var testDataPath = Path.Combine(Environment.CurrentDirectory, "Data", "recommendation-ratings-test.csv");

            IDataView trainingDataView = mlContext.Data.LoadFromTextFile<Movies>(trainingDataPath, hasHeader: true, separatorChar: ',');
            IDataView testDataView = mlContext.Data.LoadFromTextFile<Movies>(testDataPath, hasHeader: true, separatorChar: ',');

            return (trainingDataView, testDataView);
        }

        #endregion
    }
}

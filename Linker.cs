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
        /// <summary>
        /// Transforms the .csv file data to movies class data.
        /// </summary>
        /// <param name="csvPath"></param>
        /// <returns></returns>
        public static IEnumerable<Movies> CSVToMovies(string csvPath)
        {
            // Gets all lines from the .csv file.
            var lines = File.ReadAllLines(csvPath).Select(a => a.Split(',')).Skip(1);

            // Creates a list of movies.
            var movieList = new List<Movies>();

            // For each line in lines of the .csv file...
            foreach (var line in lines)
            {
                // If The length of the line is greater than 3...
                if (line.Length > 3)
                {
                    // Adds a movie to the movieList that contains:
                    movieList.Add(new Movies()
                    {
                        // A movie ID,
                        movieId = int.Parse(line[0]),
                        // A title, (This title may contains "," characters so we treat them real good.)
                        title = line.Skip(1).Take(line.Length - 2).Aggregate((x, y) => x + "," + y),
                        // And some genres separated with this "|" character.
                        genres = new List<string>(line[2].Split("|"))
                    });
                }
                // else...
                else
                {
                    // Adds a movie to the movieList that contains:
                    movieList.Add(new Movies()
                    {
                        // A movie ID,
                        movieId = int.Parse(line[0]),
                        // A title,
                        title = line[1],
                        // And some genres separated with this "|" character.
                        genres = new List<string>(line[2].Split("|"))
                    });
                }
            }
            // Returns the list of movies.
            return movieList;
        }
    }
}

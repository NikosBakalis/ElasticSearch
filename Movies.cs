using System.Collections.Generic;

namespace ElasticSearch
{
    /// <summary>
    /// Movies Class.
    /// </summary>
    public class Movies
    {
        // The movie ID.
        public int movieId { get; set; }
        
        // The title.
        public string title { get; set; }

        // The list of genres.
        public List<string> genres { get; set; }

        /// <summary>
        /// Basic constructor.
        /// </summary>
        public Movies()
        {

        }
    }
}
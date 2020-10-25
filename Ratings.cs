using System;
using System.Collections.Generic;
using System.Text;

namespace ElasticSearch
{
    /// <summary>
    /// Ratings Class.
    /// </summary>
    public class Ratings
    {
        // The user ID.
        public int userId { get; set; }

        // The movie ID.
        public int movieId { get; set; }

        // The rating.
        public float rating { get; set; }

        // The time-stamp.
        public string timestamp { get; set; }

        /// <summary>
        /// Basic constructor.
        /// </summary>
        public Ratings()
        {

        }
    }
}

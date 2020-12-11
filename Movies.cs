using System;
using System.Collections.Generic;

namespace ElasticSearch
{
    /// <summary>
    /// Movies Class.
    /// </summary>
    public class Movies : BaseDataModel<Movies>
    {
        #region Public Variables

        // The movie ID.
        public int MovieId { get; set; }

        // The title.
        public string Title { get; set; }

        // The list of genres.
        public List<string> Genres { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Basic constructor.
        /// </summary>
        public Movies()
        {

        }

        /// <summary>
        /// Standard constructor.
        /// </summary>
        /// <param name="values"></param>
        public Movies(List<int> values)
        {
            _ = values ?? throw new ArgumentNullException();

            if (values.Count != 3)
                throw new ArgumentOutOfRangeException();

            MovieId = values[0];
            Title = values[1];
            Genres = values[2];
        }

            #endregion
    }
}
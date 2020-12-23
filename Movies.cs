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
        //public Movies(List<string> values)
        //{
        //    _ = values ?? throw new ArgumentNullException();

        //    if (values.Count != 1)
        //        throw new ArgumentOutOfRangeException();

        //    Title = values[0];
        //}

            #endregion
    }
}
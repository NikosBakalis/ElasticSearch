namespace ElasticSearch
{
    /// <summary>
    /// Ratings Class.
    /// </summary>
    public class Ratings : BaseDataModel<Ratings>
    {
        #region Public Variables
        // The user ID.
        public int userId { get; set; }

        // The movie ID.
        public int movieId { get; set; }

        // The rating.
        public float rating { get; set; }

        // The time-stamp.
        public string timestamp { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Basic constructor.
        /// </summary>
        public Ratings()
        {

        }

        #endregion
    }
}

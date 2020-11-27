using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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

        #endregion
    }

    public abstract class BaseDataModel<TDataModel>
    {
        #region Private Members

        private static Lazy<PropertyInfo[]> mProperties = new Lazy<PropertyInfo[]>(() =>
        {
            return typeof(TDataModel).GetProperties();
        });

        #endregion

        #region Public Methods

        /// <summary>
        /// Returns a string that represents the current object
        /// </summary>
        /// <returns></returns>
        public override sealed string ToString()
        {
            // Create the result
            var result = string.Empty;

            // For every property...
            foreach(var property in mProperties.Value)
            {
                // Get the value
                var value = property.GetValue(this);

                result += property.Name + ": ";

                // If there is value...
                if (value == null)
                    // Continue
                    continue;

                if (property.PropertyType != typeof(string) && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
                {
                    var values = new List<object>();

                    foreach (var v in (IEnumerable)value)
                        values.Add(v);

                    result += values.Aggregate((x, y) => x + ", " + y);
                }
                else
                    result += value.ToString();

                result += " ";
            }

            // Return the result
            return result.Trim();
        }

        #endregion
    }
}
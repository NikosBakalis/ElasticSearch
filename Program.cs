using Nest;
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
            // The path of the .csv file.
            var filePath = "../../../data/movies.csv";

            // The name of the index.
            var indexName = "movies";

            // Transforms .csv file to Movies class.
            var csvToMovies = Linker.CSVToMovies(filePath);

            // Calls the Elastic Search Client.
            var elasticSearchClient = new ElasticClient();

            //  Creates the Elastic Search Client.
            //var response = await elasticSearchClient.Indices.CreateAsync(indexName);

            // Deletes the elastic search Index.
            //var delete = await elasticSearchClient.Indices.DeleteAsync(indexName);

            // Indexes the Elastic Search Index.
            //var indexResponce = await elasticSearchClient.BulkAsync(x => x.Index(indexName).IndexMany(csvToMovies));

            // Searches all the data of a specific index.
            var searchResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.MatchAll()).Index(indexName).Size(10000));

            // Searches all the data of a specific index matching a string.
            var searchStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.title).Query("(1995)"))).Index(indexName).Size(10000));
            //var searchStringResponce = await elasticSearchClient.SearchAsync<Movies>(x => x.Query(y => y.Match(z => z.Field(a => a.title).Query("(1995)"))).Index("movies").Size(10000));
        }
    }
}

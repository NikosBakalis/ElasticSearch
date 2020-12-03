namespace ElasticSearch
{
    // Class used to capture predictions.
    public class Prediction : AverageRatingPerGenre
    {
        // Original label (not used during training, just for comparison).
        public uint PredictedLabel { get; set; }
    }
}

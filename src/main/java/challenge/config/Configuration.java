package challenge.config;

public final class Configuration {
    // Fr Text data
    public static final String PLAYSTORE = "googleplaystore";
    public static final String PLAYSTORE_USER_REVIEWS = "googleplaystore_user_reviews";
    public static final String PLAYSTORE_CLEANED = "googleplaystore_cleaned";

    // For Avro+Parquet data
    public static final String PLAYSTORE_PQ = "googleplaystore_pq";
    public static final String PLAYSTORE_USER_REVIEWS_PQ = "googleplaystore_user_reviews_pq";
    public static final String PLAYSTORE_CLEANED_PQ = "googleplaystore_cleaned_pq";

    private Configuration() {}
}

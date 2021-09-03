package challenge.segments;

import challenge.RunnableSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class Part1Segment implements RunnableSegment {
    private static final Part1Segment SINGLETON = new Part1Segment();

    private Part1Segment() { }

    public static RunnableSegment of() {
        return SINGLETON;
    }

    @Override
    public Dataset<Row> run(SparkSession sparkSession) {
        return sparkSession.sql(
                "select \n" +
                        "    App,\n" +
                        "    avg( IFNULL( CAST(Sentiment_Polarity AS DECIMAL(10,2)), 0)) as Average_Sentiment_Polarity \n" +
                        "from googleplaystore_user_reviews_pq \n" +
                        "group by App"
        );
    }

}
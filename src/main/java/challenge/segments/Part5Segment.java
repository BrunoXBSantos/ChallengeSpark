package challenge.segments;

import challenge.RunnableSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class Part5Segment implements RunnableSegment {
    private static final Part5Segment SINGLETON = new Part5Segment();

    private Part5Segment() { }

    public static RunnableSegment of() {
        return SINGLETON;
    }

    @Override
    public Dataset<Row> run(SparkSession sparkSession) {
        return sparkSession.sql(
                "select \n" +
                        "App, \n" +
                        "Rating \n" +
                        "from (\n" +
                        "select \n" +
                        "    App, \n" +
                        "    IFNULL( CAST(Rating AS DECIMAL(10,2)), 0.0) AS Rating " +
                        "from googleplaystore_pq \n" +
                        ") as t2\n" +
                        "where Rating >= 4.0 " +
                        "order by Rating DESC"
        );
    }

}

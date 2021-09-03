package challenge.segments;

import challenge.RunnableSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static challenge.config.Configuration.PLAYSTORE_CLEANED_PQ;
/*
public final class Part4Segment implements RunnableSegment {
    private static final Part4Segment SINGLETON = new Part4Segment();

    private Part4Segment() { }

    public static RunnableSegment of() {
        return SINGLETON;
    }

    @Override
    public Dataset<Row> run(SparkSession sparkSession) {
        String commonColumn = "App";

        Dataset<Row> part1 = Part1Segment.of().run(sparkSession);
        Dataset<Row> part3 = Part3Segment.of().run(sparkSession);

        return part1
                .join(part3,commonColumn);
    }

}
*/

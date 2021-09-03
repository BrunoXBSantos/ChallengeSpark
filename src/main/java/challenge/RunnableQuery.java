package challenge;

import org.apache.spark.sql.SparkSession;

public interface RunnableQuery {
    void run(SparkSession sparkSession);
}

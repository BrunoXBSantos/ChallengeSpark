package challenge.Query;

import challenge.RunnableQuery;
//import challenge.segments.Part4Segment;
import com.beust.jcommander.Parameters;
import org.apache.spark.sql.SparkSession;

import static challenge.config.Configuration.PLAYSTORE_CLEANED_PQ;
    /*

@Parameters(commandDescription = "Constroi a pagina por ator")
public final class Part4Query implements RunnableQuery {
    private static final Part4Query SINGLETON = new Part4Query();

    private Part4Query() {}

    public static RunnableQuery of() {
        return SINGLETON;
    }

    @Override
    public void run(final SparkSession sparkSession) {
        Part4Segment.of().run(sparkSession)
                .write()
                .insertInto(PLAYSTORE_CLEANED_PQ);
    }

}
*/
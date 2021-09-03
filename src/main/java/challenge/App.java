package challenge;

import challenge.segments.*;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public final class App {
    private static final String PROGRAM_NAME = "challengeSpark";

    @Parameter(
            names = {"-h", "--help"},
            help = true,
            description = "Displays help information")
    private boolean help = false;

    @Parameter(
            names = {"--metastore"},
            description = "URI for Hive Metastore")
    private String metastore = "thrift://hive-metastore:9083" ;

    private static final Map<String, RunnableQuery> QUERIES = new HashMap<>();
    private static final Map<String, RunnableSegment> SEGMENTS = new HashMap<>();

    // Add all available queries
    static {
        //QUERIES.put("part4Query", Part4Query.of());

        SEGMENTS.put("part1", Part1Segment.of());
        SEGMENTS.put("part2", Part2Segment.of());
        SEGMENTS.put("part3", Part3Segment.of());
        //SEGMENTS.put("part4", Part4Segment.of());
        SEGMENTS.put("part5", Part5Segment.of());
    }

    private App() {}

    public static void main(final String[] args) {
        new App().start(args);
    }

    public void start(final String[] args) {
        String command = this.parseArguments(args);

        SparkSession sparkSession = SparkSession.builder()
                .appName("Hive tables and RDD example")
                .master("local")
                .config("hive.metastore.uris", this.metastore)
                .enableHiveSupport()
                .getOrCreate();

        try {
            QUERIES.get(command).run(sparkSession);
        } catch (NullPointerException ex) {
            SEGMENTS.get(command).run(sparkSession).show();
        }
    }

    public String parseArguments(final String[] args) {
        JCommander commands = new JCommander(this);
        commands.setProgramName(PROGRAM_NAME);

        QUERIES.forEach(commands::addCommand);
        SEGMENTS.forEach(commands::addCommand);

        try {
            commands.parse(args);

            if (this.help) {
                commands.usage();
                System.exit(0);
            }

            return commands.getParsedCommand();
        } catch (ParameterException exception) {
            System.err.println(exception.getMessage());
            commands.usage();
            System.exit(1);
        }

        return null;
    }
}
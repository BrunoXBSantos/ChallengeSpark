package challenge.segments;

import challenge.RunnableSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class Part3Segment implements RunnableSegment {
    private static final Part3Segment SINGLETON = new Part3Segment();

    private Part3Segment() { }

    public static RunnableSegment of() {
        return SINGLETON;
    }

    @Override
    public Dataset<Row> run(SparkSession sparkSession) {
        return sparkSession.sql(
                "select \n" +
                        "Distinct t4.App ,\n" +
                        "t5.Categories,\n" +
                        "t4.Rating,\n" +
                        "t4.Reviews,\n" +
                        "t4.Size,\n" +
                        "t4.Installs,\n" +
                        "t4.Type,\n" +
                        "t4.Price,\n" +
                        "t4.Content_Rating,\n" +
                        "t5.Genres,\n" +
                        "t4.Last_Updated,\n" +
                        "t4.Current_Version,\n" +
                        "t4.Minimum_Android_Version\n" +
                        "from (\n" +
                            "select \n" +
                            "   App,\n" +
                            "   Rating,\n" +
                            "   Reviews,\n" +
                            "   Size,\n" +
                            "   Installs,\n" +
                            "   Type,\n" +
                            "   Price,\n" +
                            "   ContentRating as Content_Rating,\n" +
                            "   LastUpdated as Last_Updated,\n" +
                            "   CurrentVer as Current_Version,\n" +
                            "   AndroidVer as Minimum_Android_Version,\n" +
                            "   dense_rank()\n" +
                            "   over(partition by App order by nvl(Reviews,0) desc) as rank\n" +
                            "from googleplaystore_pq\n" +
                        ") t4\n" +
                        "inner join (\n" +
                            "select \n" +
                                "App,\n" +
                                "collect_set(Category) as Categories,\n" +
                                "collect_set(Genres) as Genres\n" +
                            "from googleplaystore_pq\n" +
                            "group by App\n" +
                        ") t5\n" +
                        "on t4.App = t5.App\n" +
                        "where t4.rank <= 1\n" +
                        "order by t4.Reviews Desc"
        );
    }

}

/*

"select\n" +
                            "App,\n" +
                            "Categories,\n" +
                            "Genres\n" +
                    "from (\n" +
                        "select \n" +
                            "App,\n" +
                            "collect_list(Category) as Categories,\n" +
                            "collect_list(Genres) as Genres\n" +
                        "from googleplaystore_pq\n" +
                        "group by App\n" +
                    ") t2\n" +
                    "inner join (\n" +
                        "select \n" +
                            "App,\n" +
                            "Rating,\n" +
                            "Reviews,\n" +
                            "Size,\n" +
                            "Installs,\n" +
                            "Type,\n" +
                            "Price,\n" +
                            "ContentRating as Content_Rating,\n" +
                            "LastUpdated as Last_Updated,\n" +
                            "CurrentVer as Current_Version,\n" +
                            "AndroidVer as Minimum_Android_Version,\n" +
                            "dense_rank()\n" +
                            "over(partition by App order by Reviews desc) as rank\n" +
                        "from googleplaystore_pq as t3\n" +
                        "where \n" +
                            " t3.rank < 2\n" +
                        " group by t3.App\n" +
                    ") t4\n" +
                    "on\n" +
                        "t2.App = t4.App"


* */

/*
return sparkSession.sql(
        "select\n" +
        "    perdecade.nconst, topgen.top10generation\n" +
        "from (\n" +
        "    select\n" +
        "        nb2.nconst, floor(nb2.birthYear / 10) as decada\n" +
        "    from name_basics_pq as nb2\n" +
        ") perdecade\n" +
        "inner join (\n" +
        "    select\n" +
        "        t2.decada, collect_list(t2.nconst) as top10generation\n" +
        "    from (\n" +
        "        select\n" +
        "            nconst, decada, numFilmes, dense_rank()\n" +
        "                over(partition by decada order by numFilmes desc) as rank\n" +
        "        from (\n" +
        "            select\n" +
        "                tfilmes.*, floor(nb.birthYear / 10) as decada\n" +
        "            from (\n" +
        "                select\n" +
        "                    tp.nconst, count(*) as numFilmes\n" +
        "                from title_principals_pq as tp\n" +
        "                group by tp.nconst\n" +
        "            ) as tfilmes\n" +
        "            inner join\n" +
        "                name_basics_pq as nb\n" +
        "            on nb.nconst = tfilmes.nconst\n" +
        "            where\n" +
        "                nb.birthYear is not null\n" +
        "        ) \n" +
        "    ) t2\n" +
        "    where\n" +
        "        t2.rank < 10\n" +
        "    group by t2.decada\n" +
        ") topgen\n" +
        "on \n" +
        "    perdecade.decada = topgen.decada"
        );
*/
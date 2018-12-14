import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.jetty.server.Authentication;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

public class Spark {
    public static void deleteDirectoryRecursionJava6(File file) throws IOException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectoryRecursionJava6(entry);
                }
            }
        }
        file.delete();
    }

    public static class Title implements Serializable {
        private String Id;
        private String Title;

        public String getId() {
            return Id;
        }

        public void setId(String Id) {
            this.Id = Id;
        }

        public String getTitle() {
            return Title;
        }

        public void setTitle(String Title) {
            this.Title = Title;
        }
    }

    public static class Link implements Serializable {
        private String first_col;
        private String second_col;

        public String getfirst_col() {
            return first_col;
        }

        public void setfirst_col(String first_col) {
            this.first_col = first_col;
        }

        public String getsecond_col() {
            return second_col;
        }

        public void setsecond_col(String second_col) {
            this.second_col = second_col;
        }
    }

    public static void calculatePageRank( SparkSession spark, JavaRDD<String> lines, JavaRDD<String> titles, boolean taxation, String output, String bomb){

        JavaPairRDD<String, String> titleRdd = titles.zipWithIndex().mapToPair(s -> {
            int dub = s._2.intValue() + 1;
            String str = dub+"";
            return new Tuple2<>(str, s._1);
        });

        if(bomb.compareTo("bomb") == 0){

            Dataset<Row> titleDF = spark.createDataFrame(titleRdd.map(x -> x._1() + "," + x._2()).map(new Function<String, Title>() {
                @Override
                public Title call(String line) throws Exception {
                    String[] parts = line.split(",");
                    Title title = new Title();
                    title.setId(parts[0]);
                    if(parts.length>1){
                        title.setTitle(parts[1]);
                    }else{
                        title.setTitle("");
                    }
                    return title;
                }
            }), Title.class);
            titleDF.createOrReplaceTempView("TITLE_TABLE");

            Dataset<Row> linkDF = spark.createDataFrame(lines.map(new Function<String, Link>() {
                @Override
                public Link call(String line) throws Exception {
                    String[] parts = line.split(":\\s+");
                    Link link = new Link();
                    link.setfirst_col(parts[0]);
                    if(parts.length>1){
                        link.setsecond_col(parts[1]);
                    }else{
                        link.setsecond_col("");
                    }
                    return link;
                }
            }), Link.class);
            linkDF.createOrReplaceTempView("LINK_TABLE");

            lines = spark.sql("SELECT first_col as Id, second_col as Link FROM LINK_TABLE " +
                    "WHERE first_col IN (Select Id FROM TITLE_TABLE WHERE Title LIKE '%surfing%')")
                    .rdd().toJavaRDD().map(s -> s.toString().replaceAll("[\\[\\]]", "").replaceAll(",", ": ") + " 4290745");
        }


        JavaPairRDD<String, Iterable<String>> links = lines
                .flatMapToPair(s -> {
                    List<Tuple2<String, String>> list = new ArrayList<>();
                    if(s.split(":\\s+").length > 1){
                        for(String str: s.split(":\\s+")[1].split("\\s+")){
                            list.add(new Tuple2<>(s.split(":\\s+")[0], str));
                        }
                    }else{
                        list.add(new Tuple2<>(s.split(":\\s+")[0], ""));
                    }
                    return list.iterator();
                })
                .distinct().groupByKey().cache();

        Double e = 1/(double)links.count();
        JavaPairRDD<String, Double> ranks = links.mapValues(s -> e);
        for(int x = 0; x < 24; x++){
            JavaPairRDD<String, Double> contribs = links.join(ranks).flatMapToPair(s ->{
                List<Tuple2<String, Double>> list = new ArrayList<>();
                    for (String str : s._2()._1()) {
                        list.add(new Tuple2<>(str, s._2()._2() / Iterables.size(s._2()._1())));
                    }
//                    list.add(new Tuple2<>(s._1(),0.0));
                return list.iterator();
            });
            ranks = contribs.reduceByKey((a, b) -> (a + b));
            if(taxation) ranks = ranks.mapValues(s -> (0.15 * e) + (0.85 * s));
        }

        JavaPairRDD<String, Double> finalRdd = ranks.join(titleRdd).mapToPair(s -> new Tuple2<>(s._1() + "," + s._2()._2(), s._2()._1()));
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.parallelize(finalRdd.mapToPair(x -> x.swap())
                .sortByKey(false)
                .mapToPair(x -> x.swap())
                .take(10))
                .saveAsTextFile(output);

    }


    public static void calculatePageRank2( SparkSession spark, JavaRDD<String> lines, JavaRDD<String> titles, boolean taxation, String ouputarg){
        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        // Loads all URLs from input file and initialize their neighbors.
        Pattern SPACES = Pattern.compile(":\\s+");
        JavaPairRDD<String, Iterable<String>> links = lines.flatMapToPair(s -> {
            String[] parts = SPACES.split(s);
            List<Tuple2<String, String>> results = new ArrayList<>();
            for(String str: parts[1].split("\\s+")){
                results.add(new Tuple2<>(parts[0], str));
            }
            return results.iterator();
        }).distinct().groupByKey().cache();

        double size = links.count();
        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0/size);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < 25; current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            if(taxation){
                ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
            }else{
                ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> sum);
            }
        }

        JavaPairRDD<String, String> titleRdd = titles.zipWithIndex().mapToPair(s -> new Tuple2<>((s._2.intValue() + 1) + "", s._1));
        ranks.join(titleRdd)
              .mapToPair(s -> new Tuple2<>(s._1() + "," + s._2()._2(), s._2()._1()));

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.parallelize(ranks.mapToPair(t -> new Tuple2<Double , String>(t._2, t._1))
                .sortByKey(false)
                .mapToPair(t -> new Tuple2<String , Double>(t._2, t._1))
                .take(10))
                .saveAsTextFile(ouputarg);
    }

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void main(String[] args) throws IOException {

//        System.setProperty("hadoop.home.dir", "C:\\Users\\Sam\\workspace\\spark\\winutil\\");
//        deleteDirectoryRecursionJava6(new File(args[2]));
//        SparkConf conf = new SparkConf().setAppName("pa3").setMaster("local");

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark")
//                .master("local")
                .getOrCreate();
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        JavaRDD<String> titles = spark.read().textFile(args[1]).javaRDD();
//        JavaRDD<String> lines = sc.textFile("file:///C://Users//Sam//workspace//spark//Input.txt");
//        JavaRDD<String> titles = sc.textFile("file:///C://Users//Sam//workspace//spark//Input2.txt");

        if(args[3].trim().compareTo("bomb")!=0){
            boolean taxation = Boolean.parseBoolean(args[3]);
            calculatePageRank(spark, lines, titles, taxation, args[2], "false");
            spark.stop();
        }else {
            //4290745
            calculatePageRank(spark, lines, titles, true, args[2], "bomb");
            spark.stop();
        }
    }
}

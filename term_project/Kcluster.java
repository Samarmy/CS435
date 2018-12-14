import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Kcluster {

    public static class Coordinates implements Serializable {
        private String name;
        private double x = 0;
        private double y = 0;
        //constructor
        public Coordinates(String name, double x, double y) {
            this.name = name;
            this.x = x/10000.0;
            this.y = y;
        }

        public void setName(String name){
            this.name = name;
        }

        public void setX(double x){
            this.x = x;
        }

        public void setY(double y){
            this.y = y;
        }

        public String getName(){
            return name;
        }

        public double getX(){
            return x;
        }

        public double getY(){
            return y;
        }

        public String toString() {
            return "(" + this.name + ", " + this.x + ", " + this.y + ")";
        }

        @Override
        public int hashCode() {
            return Integer.parseInt(this.name.charAt(8) + "");
        }

        @Override
        public boolean equals(Object o) {
            return this.hashCode() == o.hashCode();
        }
    }

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

    public static double getDistance(Coordinates business, Coordinates cluster) throws IOException {
        return Math.sqrt(Math.pow(Math.abs(business.getX()-cluster.getX()),2) + Math.pow(Math.abs(business.getY()-cluster.getY()),2));
    }

    public static JavaPairRDD<Coordinates, Iterable<Coordinates>> kmeans(JavaRDD<Coordinates> businesses, int k, int maxIterations) throws IOException {
        List<Coordinates> initClusters = new ArrayList<>();
        JavaPairRDD<Coordinates, Iterable<Coordinates>> clusters = null;

        for (int m = 0; m < k; m++) {
            double randomZip = businesses.takeSample(false,1).get(0).getX();
            double randomStars = businesses.takeSample(false,1).get(0).getY();
            initClusters.add(new Coordinates("centroid" + m, (randomZip*10000.0), randomStars));
        }
        for(int x = 0; x < maxIterations; x++) {
            List<Coordinates> finalInitClusters = initClusters;
            clusters = businesses.mapToPair(y -> {
                Coordinates min = new Coordinates("x", 1000000.0, 1000000.0);
                for(Coordinates z: finalInitClusters){
                    if (getDistance(min, y) > getDistance(z, y)) {
                        min = z;
                    }
                }
                return new Tuple2<>(y, min);
            }).mapToPair(y -> y.swap()).groupByKey();

            initClusters = clusters.map(y -> {
                double x1 = 0;
                double y1 = 0;
                double count = 0;
                Iterator<Coordinates> itr = y._2().iterator();
                while (itr.hasNext()) {
                    Coordinates z = itr.next();
                    x1 += z.getX();
                    y1 += z.getY();
                    count++;
                }
                return new Coordinates(y._1().getName(), (x1*10000.0) / count, y1 / count);
            }).collect();
        }
        return clusters;
    }

    public static String getValue(char a, Map<Character, Integer> map){
        if(Character.isLetter(a)){
            return (int) (Double.parseDouble(map.get(a) + "") / (26.0 / 9.0)) + "";
        }else{
            return a + "";
        }
    }

    public static JavaRDD<Coordinates> getBuinesses(JavaRDD<String> lines, String state1, String state2){
        Map<Character, Integer> map = new HashMap<>();
        for (int i = 'A'; i <= 'Z'; i++) {
            map.put((char) i, (i - 'A' + 1));
        }
        return lines.filter(x -> {
            String name = x.split(":\\s+")[0];
            String description = x.split(":\\s+")[1];
            if(description.split("\\s+").length < 5)return false;
            String zipStr = description.split("\\s+")[1].trim();
            if(x.split(":\\s+").length > 2)return false;
            if(3 > zipStr.replaceAll("[^0-9A-Z]","").length() || 5 < zipStr.replaceAll("[^0-9A-Z]","").length()) return false;
            if(zipStr.length() == 5 && description.split("\\s+").length < 4)return false;
            if(zipStr.length() == 5 && zipStr.replaceAll("[^0-9]", "").length() != 5 )return false;
            if((zipStr.length() == 3 || zipStr.length() == 4) && description.split("\\s+").length < 5)return false;
            if(!StringUtils.isAlphanumeric(zipStr))return false;
            if (zipStr.length() == 3 || zipStr.length() == 4) {
                if (description.split("\\s+")[2].trim().length() != 3 || !StringUtils.isAlphanumeric(description.split("\\s+")[2].trim())) {
                    return false;
                }
            }
            if(state1.compareTo("ALL")!= 0) {
                if (description.split("\\s+")[0].trim().compareTo(state1) != 0 && description.split("\\s+")[0].trim().compareTo(state2) != 0) return false;
            }
            return true;
        }).map(x -> {
            String name = x.split(":\\s+")[x.split(":\\s+").length-2];
            String description = x.split(":\\s+")[x.split(":\\s+").length-1];
            String zipStr = description.split("\\s+")[1].trim();
            Double stars;
            Double reviews;
            double zipCode;
            if (zipStr.length() == 5) {
                stars = Double.parseDouble(description.split("\\s+")[2].trim());
                reviews = Double.parseDouble(description.split("\\s+")[3].trim());
                zipCode = Double.parseDouble(zipStr);
            } else if (zipStr.length() == 3) {
                stars = Double.parseDouble(description.split("\\s+")[3].trim());
                reviews = Double.parseDouble(description.split("\\s+")[4].trim());
                zipStr = zipStr + description.split("\\s+")[2].trim();
                zipCode = Double.parseDouble(getValue(zipStr.charAt(0), map) + getValue(zipStr.charAt(1), map) + getValue(zipStr.charAt(2), map) +
                        getValue(zipStr.charAt(3), map) + getValue(zipStr.charAt(4), map) + "." + getValue(zipStr.charAt(5), map));
            }else{
                stars = Double.parseDouble(description.split("\\s+")[3].trim());
                reviews = Double.parseDouble(description.split("\\s+")[4].trim());
                zipStr = zipStr + description.split("\\s+")[2].trim();
                zipCode = Double.parseDouble(getValue(zipStr.charAt(0), map) + getValue(zipStr.charAt(1), map) + getValue(zipStr.charAt(2), map) +
                        getValue(zipStr.charAt(3), map) + getValue(zipStr.charAt(4), map) + "." + getValue(zipStr.charAt(5), map) + getValue(zipStr.charAt(6), map));
            }
            return new Coordinates(name, zipCode, (stars*Math.log(reviews)/Math.log(7968.0)));
        });
    }


    public static void main(String[] args) throws IOException {
//        System.setProperty("hadoop.home.dir", "C:\\Users\\Sam\\workspace\\spark\\winutil\\");
//        deleteDirectoryRecursionJava6(new File(args[4]));
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark")
//                .master("local")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<String> lines = spark.read().textFile(args[3]).javaRDD();
        JavaRDD<Coordinates> businesses = getBuinesses(lines, args[5], args[6]).cache();
        int maxK = Integer.parseInt(args[0]);

        if(args.length == 8) {
            int iterations = Integer.parseInt(args[2]);
            List<Tuple2<Integer, Double>> sse = new ArrayList<>();
            for (int k = 1; k <= maxK; k++) {
                JavaRDD<Double> average = null;
                for (int n = 0; n < iterations; n++) {
                    JavaPairRDD<Coordinates, Iterable<Coordinates>> clusters = kmeans(businesses, k, Integer.parseInt(args[1]));
                    JavaRDD<Double> elbow = clusters.map(x -> {
                        Iterator<Coordinates> itr = x._2().iterator();
                        double total = 0;
                        while (itr.hasNext()) {
                            Coordinates next = itr.next();
                            total += Math.pow(getDistance(next, x._1()), 2);
                        }
                        return total;
                    });
                    if (n == 0) {
                        average = elbow;
                    } else {
                        average = average.union(elbow);
                    }
                }
                sse.add(new Tuple2<>(k, average.reduce((a, b) -> a + b) / k));
            }
            jsc.parallelize(sse).saveAsTextFile(args[4]);
        }else{
            kmeans(businesses, maxK, Integer.parseInt(args[1])).saveAsTextFile(args[4]);
        }
        spark.stop();
    }
}

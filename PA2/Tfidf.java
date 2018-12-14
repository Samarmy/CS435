
import java.io.*;
import java.text.BreakIterator;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

import org.apache.commons.lang3.StringUtils;

public class Tfidf {
    public static class TFMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String docId = StringUtils.substringBetween(val, "<====>", "<====>");
            StringTokenizer itr = new StringTokenizer(val.substring(val.lastIndexOf("<====>") + 1));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if (word.toString().toLowerCase().replaceAll("[^a-z0-9]", "").length() > 0) {
                    context.write(new Text(docId + "_" + word.toString().toLowerCase().replaceAll("[^a-z0-9]", "")), new IntWritable(1));
                }
            }
        }
    }

    public static class TFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TFMapper2 extends Mapper< Text, Text, Text, Text> {
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString().split("_")[0];
            String keyStr2 = key.toString().split("_")[1];
            context.write(new Text(keyStr), new Text(value.toString().trim() + "_" + keyStr2));
        }
    }

    public static class TFReducer2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int max = 0;
            ArrayList<String> cache = new ArrayList<String>();

            for (Text val : values) {
                String valStr = val.toString().split("_")[0];
                String valStr2 = val.toString().split("_")[1];
                if(max < Integer.parseInt(valStr)){
                    max = Integer.parseInt(valStr);
                }
                cache.add(val.toString());
            }
            for (String val2 : cache) {
                String val2Str = val2.toString().split("_")[0];
                String val2Str2 = val2.toString().split("_")[1];
                Double tf = (0.5 + 0.5*(Double.parseDouble(val2Str)/max));
                context.write(new Text(key.toString()), new Text(val2Str2 +"_" + tf));
            }

        }
    }

    public static class IDFMapper1 extends Mapper< Text, Text, Text, Text> {
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String valStr = value.toString().split("_")[0];
            String valStr2 = value.toString().split("_")[1];
            context.write(new Text(valStr), new Text(key + "_" + valStr2));
        }
    }

    public static class IDFReducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int ni = 0;
            ArrayList<String> cache = new ArrayList<String>();
            for (Text val : values) {
                String valStr = val.toString().split("_")[0];
                String valStr2 = val.toString().split("_")[1];
                ni++;
                cache.add(val.toString());
            }
            for (String val2 : cache) {
                String val2Str = val2.toString().split("_")[0];
                String val2Str2 = val2.toString().split("_")[1];
                context.write(new Text(val2Str), new Text(key.toString() +"_" + val2Str2 + "_" + ni ));
            }

        }
    }

    public static class IDFMapper2 extends Mapper< Text, Text, Text, NullWritable> {
        private Text word = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString().split("_")[0];
            context.write( new Text(keyStr), NullWritable.get());
        }
    }

    public static class IDFReducer2 extends Reducer<Text, NullWritable, Text, NullWritable> {
        public int count = 0;
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            count++;


        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(count + ""), NullWritable.get());
        }
    }

    public static class IDFMapper3 extends Mapper< Text, Text, Text, DoubleWritable> {
        private Text word = new Text();
        public Integer count = 0;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String line ="";
            try{
                Path pt = new Path("/testFolder/OutputIdf2/part-r-00000");//Location of file in HDFS
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br= new BufferedReader(new InputStreamReader(fs.open(pt)));
                line = br.readLine().trim();
            }catch(Exception e){
            }
            count = Integer.parseInt(line);
        }



        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {


            String valStr = value.toString().split("_")[0];
            String valStr2 = value.toString().split("_")[1];
            String valStr3 = value.toString().split("_")[2];
//            String valStr4 = value.toString().split("_")[3];
            Double tfidf = Double.parseDouble(valStr2) * Math.log10(Double.parseDouble(count.toString()) / Double.parseDouble(valStr3));
            context.write(new Text(key + "_" + valStr), new DoubleWritable(tfidf));
        }
    }

    public static class IDFReducer3 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int ni = 0;
            ArrayList<String> cache = new ArrayList<String>();

            for (Text val : values) {
                String valStr = val.toString().split("_")[0];
                String valStr2 = val.toString().split("_")[1];
                ni++;
                cache.add(val.toString());
            }
            for (String val2 : cache) {
                String val2Str = val2.toString().split("_")[0];
                String val2Str2 = val2.toString().split("_")[1];
                context.write(new Text(val2Str), new Text(key.toString() +"_" + val2Str2 + "_" + ni));
            }

        }
    }

    public static class SentenceMapper extends Mapper<Object, Text, Text, IntWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String docId = StringUtils.substringBetween(val, "<====>", "<====>");
            BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
            String text = val.substring(val.lastIndexOf("<====>") + 1);
            iterator.setText(text);

            int sentenceCounter = 0;
            String[] textAry = text.split("[.?!][ ]");
            for(String sentence: textAry){
                if(sentence.trim().length() > 0 ){
                    StringTokenizer itr = new StringTokenizer(sentence.toLowerCase().replaceAll("[^ a-z0-9]", ""));
                    while (itr.hasMoreTokens()) {
                        context.write(new Text(docId + "_" + itr.nextToken()), new IntWritable(sentenceCounter));
                    }
                    sentenceCounter++;
                }
            }
        }
    }

    public static class SentenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<String, Integer> map = new TreeMap<String, Integer>();
            for (IntWritable val : values) {

                if (!map.containsKey(val.toString())) {
                    context.write(key, val);
                    map.put(val.toString(), 1);
                }

            }
        }
    }

    public static class TFIDFJoinMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text("A_" + value.toString()));
        }
    }

    public static class SentenceJoinMapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text("B_" + value.toString()));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String strA = "";
            String strB = "";
            List<String> bList = new ArrayList<>();
            int count = 0;
            for (Text val : values) {
                if (val.charAt(0) == 'A') {
                    strA = val.toString().replace("A_", "");
                } else if (val.charAt(0) == 'B') {
                    bList.add(val.toString().replace("B_", ""));
                }
            }
            for(String str: bList){
                context.write(new Text(key.toString().split("_")[0] + "_" + str), new Text(strA));
            }
        }
    }

    public static class SentenceTFIDFMapper1 extends Mapper<Text, Text, Text, IntWritable> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if (key.toString().trim().length() > 0 && value.toString().trim().length() > 0) {
                context.write(new Text(key.toString() + "\t" + value.toString()), new IntWritable(1));
            }
        }
    }

    public static class SentenceTFIDFReducer1 extends Reducer<Text, IntWritable, Text, DoubleWritable> {//finds the top 5 tfidf's for each docid_sentence

        String keystr = "";
        int keycount = 0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                if (keystr.compareTo(key.toString().split("\\s+")[0].trim()) != 0) {
                    keystr = key.toString().split("\\s+")[0].trim();
                    context.write(new Text(keystr), new DoubleWritable(Double.parseDouble(key.toString().split("\\s+")[1].trim())));
                    keycount = 0;
                } else {
                    if (keycount < 4) {
                        context.write(new Text(keystr), new DoubleWritable(Double.parseDouble(key.toString().split("\\s+")[1].trim())));
                        keycount++;
                    }
                }
            }
        }
    }

    public static class SentenceTFIDFMapper2 extends Mapper<Text, Text, Text, DoubleWritable> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, new DoubleWritable(Double.parseDouble(value.toString())));
        }
    }

    public static class SentenceTFIDFReducer2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> { //adds top 5 tfidf values for each docid_sentenceNumber
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static class SentenceTFIDFMapper3 extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(key.toString().split("_").length > 1) {
                context.write(new Text(key.toString().split("_")[0].trim()), new Text(value.toString().trim() + "_" + key.toString().split("_")[1].trim()));
            }
        }
    }

    public static class SentenceTFIDFReducer3 extends Reducer<Text, Text, Text, Text> {//finds the top 3 tfidf and orders them from occuring first to last

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMultimap<Double, Integer> map = TreeMultimap.create(Ordering.natural().reverse(),Ordering.natural());
            for (Text val : values) {
                map.put(Double.parseDouble(val.toString().split("_")[0]), (Integer.parseInt(val.toString().split("_")[1])));
                if (map.values().size() > 3) {
                    map.remove(map.asMap().lastKey(), map.get(map.asMap().lastKey()).first());
                }
            }
            TreeMap<Integer, Double> map2 = new TreeMap<Integer, Double>();
            for (Double key1 : map.keySet()) {
                for(int num : map.get(key1))
                    map2.put(num, key1);
            }

            String value = "";
            for (Map.Entry<Integer, Double> entry : map2.entrySet()) {
                value = value + entry.getKey() + " " ;
            }

            context.write(key, new Text(value));
        }
    }

    public static class TFIDFJoinMapper2 extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.toString()), new Text("A_" + value.toString()));
        }
    }

    public static class SentenceJoinMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String docId = StringUtils.substringBetween(val, "<====>", "<====>");
            BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.US);
            String text = val.substring(val.lastIndexOf("<====>") + 1).replaceAll("====>", "");
            iterator.setText(text);


            int sentenceCounter = 0;
            String[] textAry = text.split("[.?!][ ]");

            for(String sentence: textAry) {
                if (sentence != null && sentence.trim().length() > 0 && docId != null && docId.length() > 0) {
                    context.write(new Text(docId), new Text("B_" + sentenceCounter + "___" + sentence));
                    sentenceCounter++;
                }
            }
        }
    }

    public static class JoinReducer2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> listA=new ArrayList<Integer>();
            TreeMap<Integer, String> mapB = new TreeMap<Integer, String>();
            for (Text val : values) {
                if (val.charAt(0) == 'A') {
                    for(String strA : val.toString().replace("A_", "").split(" ")){
                        listA.add(Integer.parseInt(strA));
                    }

                } else if (val.charAt(0) == 'B') {
                    String[] value = val.toString().replace("B_", "").split("___");
                    mapB.put(Integer.parseInt(value[0]), value[1]);
                }
            }
            String strC = "";
            for(int x : listA){
                String y = mapB.get(x);
                if( y.length() > 0){
                    strC += y + (y.substring(y.length()-1).compareTo(".") == 0 ? " ": ". ");
                }
            }
            context.write(new Text(key.toString()), new Text(strC));
        }
    }

    public static class SentenceTFIDFSortComparator extends WritableComparator {
        protected SentenceTFIDFSortComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String str1 = w1.toString();
            String str2 = w2.toString();
            String[] words1 = str1.split("\\s+");
            String[] words2 = str2.split("\\s+");

            return -1 * (words1[0].compareTo(words2[0]) != 0 ? words1[0].compareTo(words2[0])
                    : Double.compare(Double.parseDouble(words1[1]), Double.parseDouble(words2[1])));
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

    public static void main(String[] args) throws Exception {
//        deleteDirectoryRecursionJava6(new File("OutputTf1"));
//        deleteDirectoryRecursionJava6(new File("OutputTf2"));
//        deleteDirectoryRecursionJava6(new File("OutputIdf1"));
//        deleteDirectoryRecursionJava6(new File("OutputIdf2"));
//        deleteDirectoryRecursionJava6(new File("OutputIdf3"));
//		deleteDirectoryRecursionJava6(new File("OutputSentence"));
//		deleteDirectoryRecursionJava6(new File("OutputJoin"));
		deleteDirectoryRecursionJava6(new File("OutputSentenceTfidf1"));
		deleteDirectoryRecursionJava6(new File("OutputSentenceTfidf2"));
		deleteDirectoryRecursionJava6(new File("OutputSentenceTfidf3"));
		deleteDirectoryRecursionJava6(new File("OutputJoin2"));
        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "tf");
//        job1.getConfiguration().set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
        job1.setJarByClass(Tfidf.class);
        job1.setMapperClass(TFMapper.class);
        // job1.setSortComparatorClass(TermFrequencySortComparator.class);
        job1.setReducerClass(TFReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        Job job1_2 = Job.getInstance(conf, "tf 2");
        job1_2.setInputFormatClass(KeyValueTextInputFormat.class);
        job1_2.setJarByClass(Tfidf.class);
        job1_2.setMapperClass(TFMapper2.class);
        // job2.setSortComparatorClass(TermFrequencySortComparator.class);
        job1_2.setReducerClass(TFReducer2.class);
        job1_2.setOutputKeyClass(Text.class);
        job1_2.setOutputValueClass(Text.class);
        job1_2.setMapOutputKeyClass(Text.class);
        job1_2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1_2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1_2, new Path(args[2]));

        Job job1_3 = Job.getInstance(conf, "idf 1");
        job1_3.setInputFormatClass(KeyValueTextInputFormat.class);
        job1_3.setJarByClass(Tfidf.class);
        job1_3.setMapperClass(IDFMapper1.class);
        // job2.setSortComparatorClass(TermFrequencySortComparator.class);
        job1_3.setReducerClass(IDFReducer1.class);
        job1_3.setOutputKeyClass(Text.class);
        job1_3.setOutputValueClass(Text.class);
        job1_3.setMapOutputKeyClass(Text.class);
        job1_3.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1_3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1_3, new Path(args[3]));


        Job job1_4 = Job.getInstance(conf, "idf 2");
        job1_4.setInputFormatClass(KeyValueTextInputFormat.class);
        job1_4.setJarByClass(Tfidf.class);
        job1_4.setMapperClass(IDFMapper2.class);
        // job2.setSortComparatorClass(TermFrequencySortComparator.class);
        job1_4.setReducerClass(IDFReducer2.class);
        job1_4.setOutputKeyClass(Text.class);
        job1_4.setOutputValueClass(NullWritable.class);
        job1_4.setMapOutputKeyClass(Text.class);
        job1_4.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1_4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1_4, new Path(args[4]));


        Job job1_5 = Job.getInstance(conf, "tfidf");
        job1_5.setInputFormatClass(KeyValueTextInputFormat.class);
        job1_5.setJarByClass(Tfidf.class);
        job1_5.setMapperClass(IDFMapper3.class);
        // job2.setSortComparatorClass(TermFrequencySortComparator.class);
//        job1_5.setReducerClass(IDFReducer3.class);
        job1_5.setOutputKeyClass(Text.class);
        job1_5.setOutputValueClass(IntWritable.class);
        job1_5.setMapOutputKeyClass(Text.class);
        job1_5.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1_5, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1_5, new Path(args[5]));


        Job job2 = Job.getInstance(conf, "sentence");
        job2.setJarByClass(Tfidf.class);
        job2.setMapperClass(SentenceMapper.class);
        // job2.setSortComparatorClass(TermFrequencySortComparator.class);
        job2.setReducerClass(SentenceReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
//        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[6]));

        Job job3 = Job.getInstance(conf, "join");
        job3.setJarByClass(Tfidf.class);
        MultipleInputs.addInputPath(job3, new Path(args[5]), KeyValueTextInputFormat.class, TFIDFJoinMapper.class);
        MultipleInputs.addInputPath(job3, new Path(args[6]), KeyValueTextInputFormat.class, SentenceJoinMapper.class);
        // job3.setSortComparatorClass(TermFrequencySortComparator.class);
        job3.setReducerClass(JoinReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
//        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(args[7]));

        Job job4 = Job.getInstance(conf, "sentence tfidf 1");
        job4.setJarByClass(Tfidf.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(SentenceTFIDFMapper1.class);
        job4.setSortComparatorClass(SentenceTFIDFSortComparator.class);
        job4.setReducerClass(SentenceTFIDFReducer1.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(IntWritable.class);
        job4.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job4, new Path(args[7]));
        FileOutputFormat.setOutputPath(job4, new Path(args[8]));

        Job job5 = Job.getInstance(conf, "sentence tfidf 2");
        job5.setJarByClass(Tfidf.class);
        job5.setInputFormatClass(KeyValueTextInputFormat.class);
        job5.setMapperClass(SentenceTFIDFMapper2.class);
        // job5.setSortComparatorClass(SentenceTFIDFSortComparator.class);
        job5.setReducerClass(SentenceTFIDFReducer2.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(DoubleWritable.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(DoubleWritable.class);
//        job5.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job5, new Path(args[8]));
        FileOutputFormat.setOutputPath(job5, new Path(args[9]));

        Job job6 = Job.getInstance(conf, "sentence tfidf 3");
        job6.setJarByClass(Tfidf.class);
        job6.setInputFormatClass(KeyValueTextInputFormat.class);
        job6.setMapperClass(SentenceTFIDFMapper3.class);
        // job6.setSortComparatorClass(SentenceTFIDFSortComparator.class);
        job6.setReducerClass(SentenceTFIDFReducer3.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        job6.setMapOutputKeyClass(Text.class);
        job6.setMapOutputValueClass(Text.class);
//        job6.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job6, new Path(args[9]));
        FileOutputFormat.setOutputPath(job6, new Path(args[10]));

        Job job7 = Job.getInstance(conf, "join 2");
        job7.setJarByClass(Tfidf.class);
        MultipleInputs.addInputPath(job7, new Path(args[10]), KeyValueTextInputFormat.class, TFIDFJoinMapper2.class);
        MultipleInputs.addInputPath(job7, new Path(args[0]), TextInputFormat.class, SentenceJoinMapper2.class);
        // job3.setSortComparatorClass(TermFrequencySortComparator.class);
        job7.setReducerClass(JoinReducer2.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
//        job7.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job7, new Path(args[11]));

//        job1.waitForCompletion(true);
//        job1_2.waitForCompletion(true);
//        job1_3.waitForCompletion(true);
//        job1_4.waitForCompletion(true);
//        job1_5.waitForCompletion(true);
//		job2.waitForCompletion(true);
//		job3.waitForCompletion(true);
		job4.waitForCompletion(true);
		job5.waitForCompletion(true);
		job6.waitForCompletion(true);
        System.exit(job7.waitForCompletion(true) ? 0 : 1);

    }
}

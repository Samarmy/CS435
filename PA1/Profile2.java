import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.commons.lang3.StringUtils;
public class Profile2 {
    public static class Profile2Mapper extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String docId = StringUtils.substringBetween(val, "<====>", "<====>");
            StringTokenizer itr = new StringTokenizer(val.substring(val.lastIndexOf("<====>") + 1));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                word.set(docId + "\t" + word.toString().toLowerCase().replaceAll("[^a-z0-9]", ""));
                context.write(word, one);
            }
        }
    }

    public static class Profile2Reducer extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();
        private TreeMap < Integer,Text > repToRecordMap = new TreeMap < Integer, Text > ();
        public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Profile2Mapper2 extends Mapper < LongWritable, Text, Text, NullWritable > {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value.toString().split("\\s+").length == 3){
                context.write(value, NullWritable.get());
            }
        }
    }

    public static class Profile2Reducer2 extends Reducer < Text, NullWritable, Text, NullWritable > {
        private int count = 0;
        public void reduce(Text key, Iterable < NullWritable > values, Context context) throws IOException, InterruptedException {
            if(count < 500){
                context.write(key, NullWritable.get());
                count++;
            }
        }
    }

    public static class Profile2SortComparator extends WritableComparator {
        protected Profile2SortComparator() {
          super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String str1 = w1.toString();
            String str2 = w2.toString();
            String[] words1 = str1.split("\\s+");
            String[] words2 = str2.split("\\s+");
            return -1 * (words1[0].compareTo(words2[0]) != 0
                ? words1[0].compareTo(words2[0])
                : Integer.compare(Integer.parseInt(words1[2]), Integer.parseInt(words2[2])) != 0
                ? Integer.compare(Integer.parseInt(words1[2]), Integer.parseInt(words2[2]))
                : words1[1].compareTo(words2[1]));
        }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "profile2 1");
      job.setJarByClass(Profile2.class);
      job.setMapperClass(Profile2Mapper.class);
      // job.setCombinerClass(Profile2Reducer.class);
      // job.setPartitionerClass(Profile2Partitioner.class);
      job.setReducerClass(Profile2Reducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      // job.setNumReduceTasks(5);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      Job job2 = Job.getInstance(conf, "profile2 2");
      job2.setJarByClass(Profile2.class);
      job2.setMapperClass(Profile2Mapper2.class);
      // job2.setCombinerClass(Profile2Reducer2.class);
      job2.setSortComparatorClass(Profile2SortComparator.class);
      // job.setPartitionerClass(Profile2Partitioner2.class);
      job2.setReducerClass(Profile2Reducer2.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(NullWritable.class);
      job2.setNumReduceTasks(1);
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));

      job.waitForCompletion(true);

      System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

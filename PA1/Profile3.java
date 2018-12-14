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
public class Profile3 {
    public static class Profile3Mapper extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            StringTokenizer itr = new StringTokenizer(val.substring(val.lastIndexOf("<====>") + 1));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                word.set(word.toString().toLowerCase().replaceAll("[^a-z0-9]", ""));
                context.write(word, one);
            }
        }
    }

    public static class Profile3Partitioner extends Partitioner < Text, IntWritable > {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            if (numReduceTasks == 5) {
                char partitionKey = key.toString().toLowerCase().charAt(0);
                if (partitionKey >= 'a' && partitionKey <= 'd') {
                    return 0;
                } else if (partitionKey >= 'e' && partitionKey <= 'k') {
                    return 1;
                } else if (partitionKey >= 'l' && partitionKey <= 'q') {
                    return 2;
                } else if (partitionKey >= 'r' && partitionKey <= 'u') {
                    return 3;
                } else if (partitionKey >= 'v' && partitionKey <= 'z') {
                    return 4;
                } else {
                    return 0;
                }
            } else {
                return 0;
            }
        }
    }

    public static class Profile3Reducer extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Profile3Mapper2 extends Mapper < LongWritable, Text, IntWritable, Text > {
        private Text unigram = new Text();
        private IntWritable frequency = new IntWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strAry = value.toString().split("\\s+");
            if(strAry.length == 2 && strAry[0].trim().length() > 0){
                unigram.set(strAry[0]);
                frequency.set(Integer.parseInt(strAry[1]));
                context.write(frequency, unigram);
            }
        }
    }

    public static class Profile3Reducer2 extends Reducer < IntWritable, Text, Text, IntWritable > {
        private int count = 0;
        private String str = "";
        private Text unigram = new Text();
        private IntWritable frequency = new IntWritable();
        public void reduce(IntWritable key, Iterable < Text > values, Context context) throws IOException, InterruptedException {
          for (Text val: values) {
              str = val.toString();

            if(count < 500){
                unigram.set(str);
                context.write(unigram, key);
                count++;
            }
          }
        }
    }

    public static class Profile3SortComparator extends WritableComparator {
        protected Profile3SortComparator() {
          super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            int num1 = Integer.parseInt(w1.toString());
            int num2 = Integer.parseInt(w2.toString());
            return -1 * Integer.compare(num1,num2);
        }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Profile3 1");
      job.setJarByClass(Profile3.class);
      job.setMapperClass(Profile3Mapper.class);
      // job.setCombinerClass(Profile3Reducer.class);
      // job.setPartitionerClass(Profile3Partitioner.class);
      job.setReducerClass(Profile3Reducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      // job.setNumReduceTasks(5);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));


      Configuration conf2 = new Configuration();
      Job job2 = Job.getInstance(conf2, "Profile3 2");
      job2.setJarByClass(Profile3.class);
      job2.setMapperClass(Profile3Mapper2.class);
      // job2.setCombinerClass(Profile3Reducer2.class);
      job2.setSortComparatorClass(Profile3SortComparator.class);
      // job.setPartitionerClass(Profile3Partitioner2.class);
      job2.setReducerClass(Profile3Reducer2.class);
      job2.setMapOutputKeyClass(IntWritable.class);
      job2.setMapOutputValueClass(Text.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(IntWritable.class);
      job2.setNumReduceTasks(1);
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));

      job.waitForCompletion(true);

      System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Profile1 {
    public static class Profile1Mapper extends Mapper < Object, Text, Text, NullWritable > {
        private ArrayList < String > repToRecordMap = new ArrayList < String > ();
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().substring(value.toString().lastIndexOf("<====>") + 1));
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                word.set(word.toString().toLowerCase().replaceAll("[^a-z0-9]", ""));
                context.write(word, NullWritable.get());

            }
        }
    }

    public static class Profile1Partitioner extends Partitioner < Text, NullWritable > {
        @Override
        public int getPartition(Text key, NullWritable value, int numReduceTasks) {
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

    public static class Profile1Reducer extends Reducer < Text, NullWritable, Text, NullWritable > {
        private Text word = new Text();
        private ArrayList < String > repToRecordMap = new ArrayList < String > ();
        public void reduce(Text key, Iterable < NullWritable > values, Context context) throws IOException, InterruptedException {
            repToRecordMap.add(key.toString());
            if (repToRecordMap.size() > 500) {
                repToRecordMap.remove(repToRecordMap.remove(repToRecordMap.size() - 1));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
        InterruptedException {
            for (int x = 0; x < repToRecordMap.size(); x++) {
                word.set(repToRecordMap.get(x).toString());
                context.write(word, NullWritable.get());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Profile1.class);
        job.setMapperClass(Profile1Mapper.class);
        job.setCombinerClass(Profile1Reducer.class);
        // job.setPartitionerClass(Profile1Partitioner.class);
        job.setReducerClass(Profile1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

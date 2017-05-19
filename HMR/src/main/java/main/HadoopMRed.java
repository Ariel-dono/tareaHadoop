package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class HadoopMRed
{

    public static class HadoopMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String valueRaw = "";
            String valueKey = "";
            String valueData = "";
            while (itr.hasMoreTokens()) {
                valueRaw=itr.nextToken();
                valueKey=valueRaw.substring(1,(valueRaw.length()-1));
                word.set(valueKey);
                valueRaw=itr.nextToken();
                valueData=valueRaw.substring(0,(valueRaw.length()-1));
                one.set(Integer.parseInt(valueData));
                context.write(word, one);
            }
        }
    }

    //Reducer class
    public static class HadoopReducer extends Reducer< Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int counter = 0;
            for (IntWritable val : values) {
                sum += val.get();
                counter++;
            }
            result.set(sum/counter);
            context.write(key, result);
        }
    }


    //Main function
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average");
        job.setJarByClass(HadoopMRed.class);
        job.setMapperClass(HadoopMapper.class);
        job.setCombinerClass(HadoopReducer.class);
        job.setReducerClass(HadoopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("/home/ariel/hadoop/input/source.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/home/ariel/hadoop/output/result"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
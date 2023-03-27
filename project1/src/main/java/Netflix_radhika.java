import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix_radhika {
    public static class MyMapper1 extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            if(!value.toString().endsWith(":")){
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                int user = s.nextInt();
                int rating = s.nextInt();
                context.write(new IntWritable(user),new IntWritable(rating));
                s.close();
            }
        }
    }

    public static class MyReducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;
            for (IntWritable v: values) {
                sum += v.get();
                count++;
            };
            context.write(key,new IntWritable((int)(sum/count*10)));
        }
    }

    public static class MyMapper2 extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            int user = s.nextInt();
            int rating = s.nextInt();
            context.write(new IntWritable(rating),new IntWritable(1));
            s.close();
        }
    }

    public static class MyReducer2 extends Reducer<IntWritable,IntWritable,DoubleWritable,LongWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable v: values) {
                sum += v.get();
            };
            context.write(new DoubleWritable(key.get()/10.0) ,new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* Job1

         */
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Netflix.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);

        /* Job2

         */
        Job job2 = Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);
    }
}

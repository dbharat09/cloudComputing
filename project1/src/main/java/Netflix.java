import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {
    /* ... */

    public static class MyMapper1 extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().endsWith(":")) {
                Scanner s = new Scanner(value.toString()).useDelimiter(",");
                int x = s.nextInt();
                double y = s.nextDouble();
                context.write(new IntWritable(x), new DoubleWritable(y));
                s.close();
            }
        }
    }
    public static class MyReducer1 extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            long count=0;
            double sum=0;
            for(DoubleWritable v: values){
                count++;
                sum+=v.get();
            };
            context.write(key, new DoubleWritable(sum/(count*10)));
        }
    }

    public static class MyMapper2 extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s=new Scanner(value.toString()).useDelimiter("\t");
            int x=s.nextInt();
            double y=s.nextDouble();
            context.write(new IntWritable(1),new DoubleWritable(y));
            s.close();
        }

    }
    public static class MyReducer2 extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            double sum=0;
            for(DoubleWritable v: values){
                sum+=v.get();
            };
            context.write(new IntWritable((int)(key.get()/10.0)),new DoubleWritable(sum));
        }
    }


    public static void main ( String[] args ) throws Exception {
        Job job=Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);


        Job job2=Job.getInstance();
        job2.setJobName("MyJob2");
        job2.setJarByClass(Netflix.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true)?0:1);

    }
}

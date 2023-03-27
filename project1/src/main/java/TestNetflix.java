import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class TestNetflix {
    public static class FirstMapperClass extends Mapper<Object,Text, LongWritable, LongWritable> {
        @Override
        public void map ( Object key, Text value, Context instance )
                throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
            long user_id = scanner.nextLong();
            long follower_id = scanner.nextLong();
            instance.write(new LongWritable(follower_id),new LongWritable(user_id));
            scanner.close();
        }
    }

    public static class FirstReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        public void reduce ( LongWritable userid, Iterable<LongWritable> values, Context instance)
                throws IOException, InterruptedException {
            long numberoffollowers =0;
            for (LongWritable v: values) {
                numberoffollowers ++;
            };
            instance.write(userid,new LongWritable(numberoffollowers));
        }
    }

    public static class SecondMapperClass extends Mapper<Object,Text, LongWritable, LongWritable> {
        @Override
        public void map ( Object key, Text value, Context instance )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter("\t");
            long id = s.nextLong();
            long count = s.nextLong();
            instance.write(new LongWritable(count),new LongWritable(1));
            s.close();
        }
    }

    public static class SecondReducerClass extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context instance )
                throws IOException, InterruptedException {
            long total_value = 0;
            for (LongWritable v: values) {
                total_value++;
            };
            instance.write(key,new LongWritable(total_value));
        }
    }



    public static boolean callfirstjob(Configuration conf, String in1,String in2,String in3) throws Exception
    {
        Job First_job = Job.getInstance(conf, "FirstTwittermapreduce");
        First_job.setJarByClass(TestNetflix.class);
        First_job.setOutputKeyClass(LongWritable.class);
        First_job.setOutputValueClass(LongWritable.class);
        First_job.setMapOutputKeyClass(LongWritable.class);
        First_job.setMapOutputValueClass(LongWritable.class);
        First_job.setMapperClass(FirstMapperClass.class);
        First_job.setReducerClass(FirstReducerClass.class);
        First_job.setInputFormatClass(TextInputFormat.class);
        First_job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(First_job,new Path(in1));
        FileOutputFormat.setOutputPath(First_job,new Path(in2));

        boolean flag=First_job.waitForCompletion(true);
        return flag;



    }

    public static void callsecondjob(Configuration conf,String in1,String in2,String in3) throws Exception
    {
        Job Second_job = Job.getInstance(conf, "SecondTwittermapreduce");
        Second_job.setJarByClass(TestNetflix.class);
        Second_job.setOutputKeyClass(LongWritable.class);
        Second_job.setOutputValueClass(LongWritable.class);
        Second_job.setMapOutputKeyClass(LongWritable.class);
        Second_job.setMapOutputValueClass(LongWritable.class);
        Second_job.setMapperClass(SecondMapperClass.class);
        Second_job.setReducerClass(SecondReducerClass.class);
        Second_job.setInputFormatClass(TextInputFormat.class);
        Second_job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(Second_job,new Path(in2));
        FileOutputFormat.setOutputPath(Second_job,new Path(in3));
        Second_job.waitForCompletion(true);

    }



    public static void startjobsfortwitteranalysis(String in1,String in2, String in3) throws Exception
    {
        Configuration conf = new Configuration();
        boolean flag= callfirstjob(conf,in1,in2,in3);
        if (flag==true)

        {
            Configuration conf2 = new Configuration();
            callsecondjob(conf2,in1,in2,in3);
        }
        else
        {
            System.exit(-1);
        }
    }


    public static void main ( String[] param) throws Exception {

        startjobsfortwitteranalysis(param[0],param[1],param[2]);

    }}
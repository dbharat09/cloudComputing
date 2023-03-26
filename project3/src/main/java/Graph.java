import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/**
 * author - bxd0815
 */
class Vertex implements Writable {
    public short tag;           // 0 for a graph vertex, 1 for a group number
    public long group;          // the group where this vertex belongs to
    public long VID;            // the vertex ID
    public long[] adjacent;     // the vertex neighbors

    Vertex(short tag, long group, long VID, long[] adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex() {

    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        ArrayList<Long> aL = new ArrayList<>();
        this.adjacent = new long[aL.size()];
    }


    public void write(DataOutput out) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        out.writeInt(adjacent.length);
        for (int i = 0; i < adjacent.length; i++) {
            out.writeLong(adjacent[i]);
        }
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        int x = in.readInt();
        ArrayList<Long> aL = new ArrayList<Long>();
        for (int i = 0; i < x; i++) {
            long dummy = in.readLong();
            aL.add(dummy);
        }
        long[] a = new long[aL.size()];
        int index = 0;
        for (long value : aL) {
            a[index++] = value;
        }
        adjacent = a;
    }
}

/**
 * author - bxd0815
 */
public class Graph {

    public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {
        @Override
        public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {
            ArrayList<Long> aL = new ArrayList<>();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x = 0;
            Integer zero = 0;
            long vertexId = 0;
            Integer iVertexId = 0;
            while (s.hasNext()) {
                iVertexId = s.nextInt();
                if (x == 0) {
                    vertexId = iVertexId.longValue();
                    x = 1;
                } else {
                    long avid = iVertexId.longValue();
                    aL.add(avid);
                }
            }
            long[] a = new long[aL.size()];
            int index = 0;
            for (long v : aL) {
                a[index++] = v;
            }
            Vertex v1 = new Vertex(zero.shortValue(), vertexId, vertexId, a);
            context.write(new LongWritable(vertexId), v1);
            s.close();
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {
        @Override
        public void map(LongWritable key, Vertex v, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(v.VID),v);
            int size=v.adjacent.length;
            for(int i=0;i<size;i++)
            {
                short s=1;
                context.write(new LongWritable(v.adjacent[i]),new Vertex(s,v.group));
            }
        }

    }

    public static long getMinimumValue(long a, long b)
    {
        if(a<b) {
            return a; }
        else {
            return b; }
    }

    public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable vid, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            long m=Long.MAX_VALUE;
//            Vector<Long> adj =new Vector();
            long[] adj = new long[0];
            for(Vertex v:values)
            {
                if(v.tag==0)
                {
                    adj = v.adjacent;
                }
                m= getMinimumValue(m,v.group);
            }
            short s=0;
            context.write(new LongWritable(m),new Vertex(s,m,vid.get(),adj));
        }
    }

    public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable key, Vertex v, Context context ) throws IOException, InterruptedException {
            context.write(key,new LongWritable(1)); }
    }

    public static class Reducer3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long m=0;
            for(LongWritable v:values)
            {
                m=m+v.get();
            }
            context.write(key,new LongWritable(m)); }
    }

    public static void main(String[] args) throws Exception {
        /* ... First Map-Reduce job to read the graph */
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(Mapper1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/temp0"));
        job1.waitForCompletion(true);
        for (short i = 0; i < 5; i++) {
            /* ... Second Map-Reduce job to propagate the group number */
            Job job2 = Job.getInstance();
            job2.setJobName("MyJob2");
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2,new Path(args[1]+"/temp"+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/temp"+(i+1)));
            job2.waitForCompletion(true);
        }
        /* ... Final Map-Reduce job to calculate the connected component sizes */
        Job job3 = Job.getInstance();
        job3.setJobName("MyJob3");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Vertex.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/temp5"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Triple implements Writable {
    public int i;
    public int j;
    public double value;

    Triple () {}

    Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
        out.writeDouble(this.value);
    }

    public void readFields ( DataInput in ) throws IOException {
        i=in.readInt();
        j=in.readInt();
        value=in.readDouble();
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j+"\t"+value;
    }
}

class Block implements Writable {
    final static int rows = 100;
    final static int columns = 100;
    public double[][] data;

    Block () {
        data = new double[rows][columns];
    }

    public void write ( DataOutput out ) throws IOException {
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ ){
                out.writeDouble(data[i][j]);
            }
        }

    }

    public void readFields ( DataInput in ) throws IOException {
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ ){
                data[i][j]= in.readDouble();
            }
        }
    }

    @Override
    public String toString () {
        String s = "\n";
        for ( int i = 0; i < rows; i++ ) {
            for ( int j = 0; j < columns; j++ )
                s += String.format("\t%.3f",data[i][j]);
            s += "\n";
        }
        return s;
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair () {}

    Pair ( int i, int j ) { this.i = i; this.j = j; }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
    }

    public void readFields ( DataInput in ) throws IOException {
        i=in.readInt();
        j=in.readInt();

    }

    @Override
    public int compareTo ( Pair pair ) {
        if (i < pair.i) {
            return -1;
        }
        else if (i > pair.i) {
            return 1;
        }
        else {
            if (j < pair.j) {
                return -1;
            }
            else if (j > pair.j) {
                return 1;
            }
        }
        return 0;
    }

    @Override
    public String toString () {
        return ""+i+"\t"+j;
    }
}

public class Add extends Configured implements Tool {
    final static int rows = Block.rows;
    final static int columns = Block.columns;

    /* ... */
    public static class RenukaMapper1 extends Mapper<Object,Text,Pair,Triple>{
        @Override
        public void map ( Object key, Text line, Context context )
                throws IOException, InterruptedException {

            Scanner scan = new Scanner(line.toString()).useDelimiter(",");
            int i = scan.nextInt();
            int j = scan.nextInt();
            double v=scan.nextDouble();
            context.write(new Pair(i/rows,j/columns),new Triple(i%rows,j%columns,v));
            scan.close();
        }
    }

    public static class RenukaReducer1 extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        public void reduce ( Pair pair, Iterable<Triple> triples, Context context )
                throws IOException, InterruptedException {
            Block b=new Block();
            for (Triple t: triples)
            {   int i=t.i;
                int j=t.j;
                b.data[i][j]=t.value;
            }
            context.write(pair,b);

        }
    }
    public static class RenukaMapper11 extends Mapper<Object,Text,Pair,Triple>{
        @Override
        public void map ( Object key, Text line, Context context )
                throws IOException, InterruptedException {

            Scanner scan = new Scanner(line.toString()).useDelimiter(",");
            int i = scan.nextInt();
            int j = scan.nextInt();
            double v=scan.nextDouble();
            context.write(new Pair(i/rows,j/columns),new Triple(i%rows,j%columns,v));
            scan.close();
        }
    }

    public static class RenukaReducer11 extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        public void reduce ( Pair pair, Iterable<Triple> triples, Context context )
                throws IOException, InterruptedException {
            Block b=new Block();
            for (Triple t: triples)
            {   int i=t.i;
                int j=t.j;
                b.data[i][j]=t.value;
            }
            context.write(pair,b);
        }
    }

    public static class RenukaMapper2 extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                throws IOException, InterruptedException {
            context.write(pair,block);
        }
    }

    public static class RenukaMapper3 extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                throws IOException, InterruptedException {
            context.write(pair,block);
        }
    }

    public static class RenukaReducer2 extends Reducer<Pair,Block,Pair,Block> {
        @Override
        public void reduce ( Pair pair, Iterable<Block> blocks, Context context )
                throws IOException, InterruptedException {
            Block s=new Block();
            for (Block b:blocks){
                for ( int i = 0; i < rows; i++ ) {
                    for ( int j = 0; j < columns; j++ ){
                        s.data[i][j] += b.data[i][j];
                    }
                };
            }
            context.write(pair,s);
        }
    }
    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Add(),args);
        Configuration conf = new Configuration();
        /*
        Job 1
         */
        Job job1 = Job.getInstance(conf,"Job1");
        job1.setJarByClass(Add.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(Triple.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RenukaMapper1.class);
        job1.setMapperClass(RenukaMapper1.class);
        job1.setReducerClass(RenukaReducer1.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(Block.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        Configuration conf11 = new Configuration();
        /*
        job 1-2
         */
        Job job11 = Job.getInstance(conf,"Job11");
        job11.setJarByClass(Add.class);
        job11.setMapOutputKeyClass(Pair.class);
        job11.setMapOutputValueClass(Triple.class);
        MultipleInputs.addInputPath(job11, new Path(args[1]), TextInputFormat.class, RenukaMapper11.class);
        job11.setMapperClass(RenukaMapper11.class);
        job11.setReducerClass(RenukaReducer11.class);
        job11.setOutputKeyClass(Pair.class);
        job11.setOutputValueClass(Block.class);
        job11.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job11, new Path(args[3]));
        job11.waitForCompletion(true);

        /*
        job 2
         */
        Job job2 = Job.getInstance(conf,"Job2");
        job2.setJarByClass(Add.class);
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, RenukaMapper2.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, RenukaMapper3.class);
        job2.setMapperClass(RenukaMapper2.class);
        job2.setMapperClass(RenukaMapper3.class);
        job2.setReducerClass(RenukaReducer2.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(Block.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job2, new Path(args[2]));
        SequenceFileInputFormat.addInputPath(job2, new Path(args[3]));
        SequenceFileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        job2.waitForCompletion(true);
    }
}

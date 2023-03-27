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

    public static class FirstMapper extends Mapper<Object,Text,Pair,Triple>{
        @Override
        public void map ( Object key, Text line, Context context )
                throws IOException, InterruptedException {

            Scanner scannedLine = new Scanner(line.toString()).useDelimiter(",");
            int i = scannedLine.nextInt();
            int j = scannedLine.nextInt();
            double v=scannedLine.nextDouble();
            Pair p = new Pair(i/rows,j/columns);
            Triple t = new Triple(i%rows,j%columns,v);
            context.write(p,t);
            scannedLine.close();
        }
    }

    public static class FirstReducer extends Reducer<Pair,Triple,Pair,Block> {
        @Override
        public void reduce ( Pair pair, Iterable<Triple> triples, Context context )
                throws IOException, InterruptedException {
            Block b=new Block();
            for (Triple t: triples)
                b.data[t.i][t.j]=t.value;
            context.write(pair,b);

        }
    }

    public static class SecondMapper extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                throws IOException, InterruptedException {
            context.write(pair,block);
        }
    }

    public static class SecondMapper2 extends Mapper<Pair,Block,Pair,Block>{
        @Override
        public void map ( Pair pair, Block block, Context context )
                throws IOException, InterruptedException {
            context.write(pair,block);
        }
    }

    public static class SecondReducer extends Reducer<Pair,Block,Pair,Block> {
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
        Configuration conf = new Configuration();
        Job firstJob = Job.getInstance(conf,"firstJob");
        firstJob.setJarByClass(Add.class);
        firstJob.setMapOutputKeyClass(Pair.class);
        firstJob.setMapOutputValueClass(Triple.class);
        System.out.println(args[0]);
        MultipleInputs.addInputPath(firstJob, new Path(args[0]), TextInputFormat.class, FirstMapper.class);
        firstJob.setMapperClass(FirstMapper.class);
        firstJob.setReducerClass(FirstReducer.class);
        firstJob.setOutputKeyClass(Pair.class);
        firstJob.setOutputValueClass(Block.class);
        firstJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(firstJob, new Path(args[2]));
        firstJob.waitForCompletion(true);

        Configuration conf11 = new Configuration();
        Job firstJob1 = Job.getInstance(conf,"FirstJob1");
        firstJob1.setJarByClass(Add.class);
        firstJob1.setMapOutputKeyClass( Pair.class);
        firstJob1.setMapOutputValueClass(Triple.class);
        MultipleInputs.addInputPath(firstJob1, new Path(args[1]), TextInputFormat.class, FirstMapper.class);
        firstJob1.setMapperClass(FirstMapper.class);
        firstJob1.setReducerClass(FirstReducer.class);
        firstJob1.setOutputKeyClass(Pair.class);
        firstJob1.setOutputValueClass(Block.class);
        firstJob1.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(firstJob1, new Path(args[3]));
        firstJob1.waitForCompletion(true);

        //Job_2 configurations
        Job secondJob = Job.getInstance(conf,"SecondJob");
        secondJob.setJarByClass(Add.class);
        MultipleInputs.addInputPath(secondJob, new Path(args[2]), TextInputFormat.class, SecondMapper.class);
        MultipleInputs.addInputPath(secondJob, new Path(args[3]), TextInputFormat.class, SecondMapper2.class);
        secondJob.setMapperClass(SecondMapper.class);
        secondJob.setMapperClass(SecondMapper2.class);
        secondJob.setReducerClass(SecondReducer.class);
        secondJob.setMapOutputKeyClass(Pair.class);
        secondJob.setMapOutputValueClass(Block.class);
        secondJob.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(secondJob, new Path(args[2]));
        SequenceFileInputFormat.addInputPath(secondJob, new Path(args[3]));
        SequenceFileOutputFormat.setOutputPath(secondJob, new Path(args[4]));
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(secondJob, new Path(args[4]));
        secondJob.waitForCompletion(true);
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Add(),args);

    }
}

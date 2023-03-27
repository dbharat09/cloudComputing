import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
    Pair ( int i, int j ) { this.i = i; this.j = j;}

	public int compareTo ( Pair p){
        int is_same = Integer.compare(i, p.i);
        //if not equal to 0 return value
        if (is_same != 0){
            return is_same;
        }
        is_same = Integer.compare(j, p.j);
        return is_same;
    }

    public void readFields (DataInput in) throws IOException{
	    i = in.readInt();
	    j = in.readInt();
    }

	public String toString () { return String.valueOf(i);} //Get the index

	public void write (DataOutput out) throws IOException{
	    out.writeInt(i);
	    out.writeInt(j);
    }

}

class Value implements Writable {
    
    public short row;  
    public int column;  
    // e represents the values
    public double e;
    
    Value () {}

    Value (short row, int column, double e){
        this.row = row;
        this.column = column;
        this.e = e;
    }
    
    @Override
    public void readFields (DataInput in) throws IOException {
        this.row = in.readShort();
        this.column = in.readInt();
        this.e = in.readDouble();
    }

	@Override
    public void write (DataOutput out) throws IOException {
        out.writeShort(this.row);
        out.writeInt(this.column);
        out.writeDouble(this.e);
    }
  }


public class Add extends Configured implements Tool {

    // Mapper for matrix 1
	public static class ARD5625Mapper1 extends Mapper<Object, Text, IntWritable, Value>{
	
		@Override
		public void map(Object key, Text value, Context context )
		throws IOException, InterruptedException{
			Scanner read = new Scanner(value.toString()).useDelimiter(",");
			int i = read.nextInt();
			int j = read.nextInt();
			double element = read.nextDouble();
			short row = 0;
			Value v = new Value (row, i, element);
			//Emit
			context.write(new IntWritable(j), v);
			read.close();
		}	
	}

    // Mapper for matrix 2
	public static class ARD5625Mapper2 extends Mapper<Object, Text, IntWritable, Value>{
		@Override
		public void map(Object key, Text value, Context context )
		throws IOException, InterruptedException {
			Scanner read = new Scanner(value.toString()).useDelimiter(",");
			int i = read.nextInt();
			int j = read.nextInt();
			double element  = read.nextDouble();
			short row = 1;
			Value v = new Value (row, i, element);
			// Emit
			context.write(new IntWritable(j), v);
			read.close();
		}
	}

	//Reducer 1 for job 1
	public static class ARD5625Reducer1 extends Reducer<IntWritable,Value,Pair,DoubleWritable> {
        ArrayList<Value> Matrix1 = new ArrayList<Value>();
        ArrayList<Value> Matrix2 = new ArrayList<Value>();
        @Override
        public void reduce ( IntWritable key, Iterable<Value> values, Context context )
                           throws IOException, InterruptedException {
            Matrix1.clear();
            Matrix2.clear();
            for (Value element: values){
                if (element.row == 0){
                    Matrix1.add(new Value(element.row, element.column, element.e));
                }       
				else{
					Matrix2.add(new Value(element.row, element.column, element.e));
				}   
            };
                 
            System.out.println("Matrix 1: "+Matrix1);
            System.out.println("Matrix 2: "+Matrix2);
            for ( Value m1: Matrix1 )
			{
				for ( Value m2: Matrix2 )
				{
					if (m1.column== m2.column){
					context.write(new Pair (m1.column, m2.column),new DoubleWritable(m1.e + m2.e));
					}
				}
			}
        }
    }

    //Mapper for job 2 
	public static class ARD5625Mapper3 extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable>{
		public void map(Pair key, DoubleWritable value, Context context)
		throws IOException, InterruptedException{

			context.write(key, value);
		}
	}

    //Reducer for job 2
	public static class ARD5625Reducer2 extends Reducer<Pair, DoubleWritable, Text, DoubleWritable >{
		@Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double num = 0;
            for (DoubleWritable val: values) {
                num = num + val.get();
            };
            context.write(new Text (key.toString()),new DoubleWritable (num));
        }
	} 

    @Override
    public int run ( String[] args ) throws Exception {return 0;}

    public static void main ( String[] args ) throws Exception {
		// Job 1 
        Job ARD5625J1 = Job.getInstance();
        ARD5625J1.setJobName("ARD5625J1");
        ARD5625J1.setJarByClass(Add.class);
        ARD5625J1.setOutputKeyClass(Pair.class);
        ARD5625J1.setOutputValueClass(DoubleWritable.class);
        ARD5625J1.setMapOutputKeyClass(IntWritable.class);
        ARD5625J1.setMapOutputValueClass(Value.class);
        ARD5625J1.setReducerClass(ARD5625Reducer1.class);
        ARD5625J1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(ARD5625J1,new Path(args[0]),TextInputFormat.class,ARD5625Mapper1.class);
        MultipleInputs.addInputPath(ARD5625J1,new Path(args[1]),TextInputFormat.class,ARD5625Mapper2.class);
        FileOutputFormat.setOutputPath(ARD5625J1,new Path(args[2]));
        ARD5625J1.waitForCompletion(true);
        
        // Job 2
        Job ARD5625J2 = Job.getInstance();
        ARD5625J2.setJobName("ARD5625J2");
        ARD5625J2.setJarByClass(Add.class);
        ARD5625J2.setMapperClass(ARD5625Mapper3.class);
        ARD5625J2.setReducerClass(ARD5625Reducer2.class);
        ARD5625J2.setOutputValueClass(DoubleWritable.class);
        ARD5625J2.setMapOutputKeyClass(Pair.class);
        ARD5625J2.setMapOutputValueClass(DoubleWritable.class);
        ARD5625J2.setInputFormatClass(SequenceFileInputFormat.class);
        ARD5625J2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(ARD5625J2, new Path(args[2]));
        FileOutputFormat.setOutputPath(ARD5625J2,new Path(args[3]));
        ARD5625J2.waitForCompletion(true);
    }
}

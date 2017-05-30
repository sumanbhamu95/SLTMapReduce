package NYSESLT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ReduceSideJoin {

	// find total num of transaction and value of those transaction for each
	// customer

	// cust
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			context.write(new Text(arr[0]), new Text("cust\t" + arr[1]));// key-cust
			// id,value
			// is-name

		}
	}

	// sale--transaction
	public static class MyMapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			
			context.write(new Text(arr[2]), new Text("tran\t" + arr[3]));// key-cust
			// id
			// ,value
			// is-amt
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

		
			String name="";
			float total=0.0f;
			int count =0;
			for (Text s : value) {

				String parts[]=s.toString().split("\t");
				if(parts[0].equals("tran")){
					
					
					count++;
					total +=Float.parseFloat(parts[1]);
				}
				else if(parts[0].equals("cust")){
					name=parts[1];
				}
				
			}
			String str=count+","+total;
			

			context.write(new Text(name), new Text(str));
			
			

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = Job.getInstance(c, "reduce side join");
		job.setJarByClass(ReduceSideJoin.class);
		

		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		FileSystem.get(c).delete(new Path(args[2]), true);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, MyMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, MyMapper1.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

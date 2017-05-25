package NYSESLT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxVarience {

	/**
	 * max varience
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			float sd=((Float.parseFloat(arr[4])-Float.parseFloat(arr[5]))/Float.parseFloat(arr[5]))*100;//variance=((high-low)/low)*100
			context.write(new Text(arr[1]),new FloatWritable(sd) );
		}
	}
		public static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
			public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
				
				float s=0;
				for(FloatWritable d:value){
					s=Math.max(s, d.get());//retrieving max variance value
				}
				context.write(key, new FloatWritable(s));
				
			}
		}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "MaxVarience.....");
			job.setJarByClass(MaxVarience.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			 job.setNumReduceTasks(2);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(obj).delete(new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}



}

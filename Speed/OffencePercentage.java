package NYSESLT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OffencePercentage {

	/**
	 * find the no. plates and offence %
	 * offence ==  more than 65Kmph
	 * final output
	 * ............
	 * number plate,30%
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			context.write(new Text(arr[0]),new IntWritable(Integer.parseInt(arr[1])) );
		}
	}
		public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
			public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
				
				int off=0,c=0,per=0;
				for(IntWritable d:value){
					c++;
					if(d.get()>65){
						off++;
					}
					
				}
				per=(off*100/c);
			//	String ss=per+"%";
				context.write(key, new IntWritable(per));
				
			}
		}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "offence percent.....");
			job.setJarByClass(OffencePercentage.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			// job.setNumReduceTasks(2);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(obj).delete(new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}




}

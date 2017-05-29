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

public class SearchString {

	/**
	 * search string --occured in the sentence and count
	 * 
	 * 
	 */

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr = value.toString();
			String newArr = arr.toLowerCase();//file se value

			String searchT = context.getConfiguration().get("myTextt");
			String newStr = searchT.toLowerCase();//search string

			if (searchT != null) {
				if (newArr.contains(newStr)) {
					context.write(new Text(newArr), new IntWritable(1));
				}
			}

		}
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value,
				Context context) throws IOException, InterruptedException {

			int c = 0;
			for (IntWritable d : value) {
				c=c+d.get();//sum kar rahe hai

			}

			context.write(key, new IntWritable(c));

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration con = new Configuration();
		if (args.length > 2) {
			con.set("myTextt", args[2]);
		} else {
			System.out.println("number of arguments must b 3");
			System.exit(0);
		}
		Job job = Job.getInstance(con, "String search");
		job.setJarByClass(SearchString.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(con).delete(new Path(args[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

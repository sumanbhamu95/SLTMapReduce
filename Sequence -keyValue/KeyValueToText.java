package NYSESLT;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KeyValueToText {

	/**
	 * @param args
	 * @throws IOException
	 */

	public static class KeyValueMapper extends
			Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		c.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

		Job job = Job.getInstance(c,
				"break string into 2 parts i.e key and value ");
		job.setJarByClass(KeyValueToText.class);
		job.setMapperClass(KeyValueMapper.class);

		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem.get(c).delete(new Path(args[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

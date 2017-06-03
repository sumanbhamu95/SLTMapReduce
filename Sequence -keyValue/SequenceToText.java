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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SequenceToText {

	/**
	 * @param args
	 */
	
	public static class SequenceMapper extends
	Mapper<LongWritable, Text, LongWritable, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	
	context.write(key, value);
	
}
}

public static void main(String[] args) throws Exception {
Configuration c = new Configuration();

Job job = Job.getInstance(c,
		"convert  sequence  to text");
job.setJarByClass(SequenceToText.class);
job.setMapperClass(SequenceMapper.class);

job.setNumReduceTasks(0);
job.setOutputKeyClass(LongWritable.class);
job.setOutputValueClass(Text.class);


job.setInputFormatClass(SequenceFileInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

FileSystem.get(c).delete(new Path(args[1]), true);
System.exit(job.waitForCompletion(true) ? 0 : 1);

}


	

}

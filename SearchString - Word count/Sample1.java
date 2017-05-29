import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public  class Sample1 {

	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String name=null;int i=1;
			
			
			/*StringTokenizer itr=new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String myword=itr.nextToken().toLowerCase();
				context.write(new Text(myword),new IntWritable(i));//o/p format
			}*/
			
			String arr[]=value.toString().split(",");
			for(String a:arr){
				name=a; 	
			context.write(new Text(name),new IntWritable(i));//o/p format
			}
		}
		
	}
	//reducer --hs only one mthd -reduce
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable i:value){
				sum=sum+i.get();//get mthd converts IntWitable--int
				
			}
			context.write(key, new IntWritable(sum));
			
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration(); // like web.xml
		Job job=Job.getInstance(c,"wordcount");//c-reference of configuration and wordcount-userdefined var
		job.setJarByClass(Sample1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem.get(c).delete(new Path(args[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

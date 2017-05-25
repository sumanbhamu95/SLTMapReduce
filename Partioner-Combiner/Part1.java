package NYSESLT;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Part1 {
public static class coun extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			String arr[]=value.toString().split(",");
		
			context.write(new Text(arr[3]), new Text((arr[2]+","+arr[4])));
		}
	}
public static  class pa extends Partitioner<Text,Text>{
	
		
		@Override
		public int getPartition(Text key, Text value, int arg2) {
			// TODO Auto-generated method stub
			String ss[]=value.toString().split(",");
			int age=Integer.parseInt(ss[0]);
		
		if(age<=20){
			return 0;
		}
		if(age>20 && age<30){
			return 1;
		}
		if(age>30){
			return 2;
		}
		else{
			return 3;
		}
	}

}
	


	public static class re extends Reducer<Text,Text,Text,Text>
	{  
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
		{
	 	    int sum=0;int max=0;	String str="";
	 	   for(Text s:value){
				String ss[]=s.toString().split(",");
			int age =Integer.parseInt(ss[0]);
			int sal=Integer.parseInt(ss[1]);
				
				if(sal>max){
					max=sal;
				}
			 str=age+"  "+max;

		 		}
	 	   
			context.write(key, new Text(str));

			}
	 		
	 		
	 	}
		
		

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Part1.class);
		job.setMapperClass(coun.class);
		job.setPartitionerClass(pa.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(4);

		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		
		
	}



	


}

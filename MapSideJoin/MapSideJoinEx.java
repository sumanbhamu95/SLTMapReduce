package NYSESLT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoinEx {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	
	
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	
	private Map<String,String> abMap=new HashMap<String,String>();
	private Map<String,String> abMap1=new HashMap<String,String>();
	private Text outputkey=new Text();
	private Text outputvalue=new Text();
	
		
		
		public void setup(Context context) throws IOException, InterruptedException{
			
			super.setup(context);
			URI[] files=context.getCacheFiles();//getCachefile return null
			
			Path p=new Path(files[0]);  //getting 1st file from hadoop fs -put salary.txt /user/hduser
			
			Path p1=new Path(files[1]);  //getting 1st file from hadoop fs -put desig.txt /user/hduser
			
			if(p.getName().equals("salary.txt")){
				BufferedReader reader=new BufferedReader(new FileReader(p.toString()));
				String line=reader.readLine();
				while(line!=null){
					String[] tokens = line.split(",");
					String emp_id=tokens[0];
					String emp_sal=tokens[1];
					abMap.put(emp_id, emp_sal);
					line=reader.readLine();
				}
				reader.close();
			}
			if(p1.getName().equals("desig.txt")){
				BufferedReader reader=new BufferedReader(new FileReader(p1.toString()));
				String line=reader.readLine();
				while(line!=null){
					String[] tokens=line.split(",");
					String emp_id=tokens[0];
					String emp_desig=tokens[1];
					abMap1.put(emp_id, emp_desig);
					line=reader.readLine();
					
				}
				reader.close();
			}
			if(abMap.isEmpty()){
				throw new IOException("MyError:unable to load salary data");
			}
				
		}
		
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
		
			String row=value.toString();
			String[] tokens=row.split(",");
			String emp_id=tokens[0];
			String salary=abMap.get(emp_id);
			String desig=abMap1.get(emp_id);
			String sal_desig=salary+","+desig;
			
			outputkey.set(row);
			outputvalue.set(sal_desig);
			
			context.write(outputkey,outputvalue);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration con=new Configuration();
		con.set("mapreduce.output.textoutputformat.separator", ",");
		Job job=Job.getInstance(con);
		job.setJarByClass(MapSideJoinEx.class);
		job.setJobName("map side join");
		job.setMapperClass(MyMapper.class);
		job.addCacheFile(new Path("salary.txt").toUri());
		job.addCacheFile(new Path("desig.txt").toUri());
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		 FileSystem.get(con).delete(new Path(args[1]),true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	
	}

}

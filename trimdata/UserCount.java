package usercount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UserCount {
	
	@SuppressWarnings("deprecation")
	public static void main (String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		
	String inputPath = args[0];
	String outputPath = args[1];
	
	String UserPostCount = outputPath + "UserPostCount";
//	String DBUserDateGlobal = outputPath + "DatabaseMinMax";

//	Configuration conf = new Configuration();
	
	Job job = new Job();
	job.setJarByClass(UserCount.class);
	job.setJobName("UserPostCount");

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(UserPostCountMapper.class);
	job.setReducerClass(UserPostCountReducer.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(inputPath));
	FileOutputFormat.setOutputPath(job, new Path(UserPostCount));	
	
	job.waitForCompletion(true);
	}
}

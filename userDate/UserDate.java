package userdate;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class UserDate {
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];
		
		String DBUserDate = outputPath + "UserMinMax";
		String DBUserDateGlobal = outputPath + "DatabaseMinMax";
		String DBUserDateMerged = outputPath + "MinMaxMerged";

//		Configuration conf = new Configuration();
		Job job = new Job();
		job.setJarByClass(UserDate.class);
		job.setJobName("UserMinMax");

		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(DBUserMinMaxMapper.class);
		job.setReducerClass(DBUserMinMaxReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(DBUserDate));	
		
		job.waitForCompletion(true);
		
		Job job1 = new Job();
		job1.setJarByClass(UserDate.class);
		job1.setJobName("DatabaseMinMax");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(DBGlobalMinMaxMapper.class);
		job1.setReducerClass(DBGlobalMinMaxReducer.class);
		
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(DBUserDate));
		FileOutputFormat.setOutputPath(job1, new Path(DBUserDateGlobal));	
		
		job1.waitForCompletion(true);
		
		Job job2 = new Job();
		job2.setJarByClass(UserDate.class);
		job2.setJobName("MinMaxMerged");
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(MergeMapper.class);
		job2.setReducerClass(MergeReducer.class);
		
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(DBUserDate));
		FileInputFormat.addInputPath(job2, new Path(DBUserDateGlobal));
		FileOutputFormat.setOutputPath(job2, new Path(DBUserDateMerged));	
		
		job2.waitForCompletion(true);
	
	}

}

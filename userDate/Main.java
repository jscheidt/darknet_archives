package gp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Main {
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		String inputPath = args[0];
		String outputPath = args[1];
		
		String DBUserDate = outputPath + "/DBUserDate";
		String DBUserDateGlobal = outputPath + "/DBUserDateGlobal";

		Configuration conf = new Configuration();
		Job job = new Job(conf, "DBUserDate");

		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(DBUserDateMapper.class);
		job.setReducerClass(DBUserDateReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(DBUserDate));	
		
		job.waitForCompletion(true);
		
		Job job1 = new Job(conf, "DBUserDateGlobal");
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setMapperClass(DBLastPostMapper.class);
		job1.setReducerClass(DBLastPostReducer.class);
		
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(DBUserDate));
		FileOutputFormat.setOutputPath(job1, new Path(DBUserDateGlobal));	
		
		job1.waitForCompletion(true);
	
	}

}

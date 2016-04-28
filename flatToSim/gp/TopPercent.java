package gp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopPercent {

	  public static void main(String[] args) throws Exception {
		  
		  	//Input should be the result of CosineCountReducer
		  
		    String inputPath = args[0];
		    String outputPath = args[1];
		    
		    String job1OutputPath = outputPath+"top";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(TopPercent.class);
		    job1.setJobName("compress Volumes");

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(TopPercentMapper.class);
		    job1.setReducerClass(TopPercentReducer.class);
		    
		    job1.setMapOutputKeyClass(IntWritable.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);

		    boolean success = job1.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
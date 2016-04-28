package gp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Magnitude {

	  public static void main(String[] args) throws Exception {
		  
		  	//Input should be the tfXidf valus from the flat dataset.
		  
		    String inputPath = args[0];
		    String outputPath = args[1];
		    
		    String job1OutputPath = outputPath+"calculateMagnitude";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(Magnitude.class);
		    job1.setJobName("compress Volumes");

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(MagnitudeMapper.class);
		    job1.setReducerClass(MagnitudeReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(DoubleWritable.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(DoubleWritable.class);

		    boolean success = job1.waitForCompletion(true);
		    
		    @SuppressWarnings("deprecation")
			Job magnitudeMultiply = new Job();
		    magnitudeMultiply.setJarByClass(Magnitude.class);
		    magnitudeMultiply.setJobName("multiplyMagnitude");
		    
		    String magMultPath = outputPath+"multiplyMag";

	        FileInputFormat.addInputPath(magnitudeMultiply, new Path(job1OutputPath));
		    FileOutputFormat.setOutputPath(magnitudeMultiply, new Path(magMultPath));
		

		    magnitudeMultiply.setMapperClass(MultiplyMagnitudeMapper.class);
		    magnitudeMultiply.setReducerClass(MultiplyMagnitudeReducer.class);
		    
		    magnitudeMultiply.setMapOutputKeyClass(IntWritable.class);
		    magnitudeMultiply.setMapOutputValueClass(Text.class);
		    magnitudeMultiply.setOutputKeyClass(Text.class);
		    magnitudeMultiply.setOutputValueClass(DoubleWritable.class);
		    
		    success = magnitudeMultiply.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
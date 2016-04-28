package gp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CalcSimilarity {

	  public static void main(String[] args) throws Exception {
		  
		  	//Input should be the result of SumDotProductReducer and MultiplyMagnitudeReducer
		  
		    String inputPath = args[0];
		    String inputPath2 = args[1];
		    String outputPath = args[2];
		    
		    String job1OutputPath = outputPath+"calcSim";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(CalcSimilarity.class);
		    job1.setJobName("calculate similarity");

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
	        FileInputFormat.addInputPath(job1, new Path(inputPath2));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(CalcSimilarityMapper.class);
		    job1.setReducerClass(CalcSimilarityReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(DoubleWritable.class);

		    boolean success = job1.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
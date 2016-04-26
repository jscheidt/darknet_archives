package sumVectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumDotProduct {

	  public static void main(String[] args) throws Exception {
		  
		  	//Input should be the result of CosineCountReducer
		  
		    String inputPath = args[0];
		    String outputPath = args[1];
		    
		    String job1OutputPath = outputPath+"sumDotProduct";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(SumDotProduct.class);
		    job1.setJobName("compress Volumes");

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(SumDotProductMapper.class);
		    job1.setReducerClass(SumDotProductReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(DoubleWritable.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(DoubleWritable.class);

		    boolean success = job1.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
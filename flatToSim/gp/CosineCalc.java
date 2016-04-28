package gp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CosineCalc {

	  public static void main(String[] args) throws Exception {
		    
		    String inputPath = args[0];
		    String outputPath = args[1];
			//String datasetPath = args[2];
		    //String searchIDFoutput = datasetPath + "searchIDF";
		    //String datasetTFIDFpath = datasetPath;
		    
		    String job1OutputPath = outputPath+"cosine001";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(CosineCalc.class);
		    job1.setJobName("compress Volumes");

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(CosineCountMapper.class);
		    job1.setReducerClass(CosineCountReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(DoubleWritable.class);

		    boolean success = job1.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
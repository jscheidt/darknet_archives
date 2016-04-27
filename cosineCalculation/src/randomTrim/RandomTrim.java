package randomTrim;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RandomTrim {

	  public static void main(String[] args) throws Exception {
		    
		  	//Takes flat as an input
		    String inputPath = args[0];
		    String outputPath = args[1];
		    
		    String job1OutputPath = outputPath+"randomTrim";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(RandomTrim.class);
		    job1.setJobName("randomly removes 3/4 users");
		    job1.setNumReduceTasks(10);

	        FileInputFormat.addInputPath(job1, new Path(inputPath));
		    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
		

		    job1.setMapperClass(RandomTrimMapper.class);
		    job1.setReducerClass(RandomTrimReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);

		    boolean success = job1.waitForCompletion(true);
		    System.exit(success ? 0 : 1);
	  }
}
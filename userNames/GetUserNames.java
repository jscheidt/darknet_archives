package gp2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetUserNames {

	  public static void main(String[] args) throws Exception {

	    //int numReduceTasks = 50;
	    
	    String inputPath = args[0];
	    String job1OutputPath = args[1]+"userList";

	    @SuppressWarnings("deprecation")
		Job job1 = new Job();
	    job1.setJarByClass(GetUserNames.class);
	    job1.setJobName("compress Volumes");

        FileInputFormat.addInputPath(job1, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
	    
	    //job1.setInputFormatClass(WholeFileInputFormat.class);
	    
	    //job1.setNumReduceTasks(numReduceTasks);

	    job1.setMapperClass(GetUserNamesMapper.class);
	    job1.setReducerClass(GetUserNamesReducer.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    boolean success = job1.waitForCompletion(true);

	    System.exit(success ? 0 : 1);
	  }
}

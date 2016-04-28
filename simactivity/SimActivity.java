package simactivity;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import top.TopPercent;
import top.TopPercentMapper;
import top.TopPercentReducer;


public class SimActivity {

	  public static void main(String[] args) throws Exception {
		  
		  	//Input should be the top similar list for args[0]
		  	//input should be the user activity file for args[1]
		  	
		    String cosSimTop = args[0];
		    String userActivity = args[1];
		    String outputPath = args[2];
		    
		    String halfMerged = outputPath+"halfMerged";
		    String fullMerged = outputPath+"fullMerged";
		    String sortedMerged = outputPath+"sortedMerged";
		    
		    @SuppressWarnings("deprecation")
			Job job1 = new Job();
		    job1.setJarByClass(SimActivity.class);
		    job1.setJobName("Merge sim values with user activity");

		    job1.setMapperClass(SimActivityMapper.class);
		    job1.setReducerClass(SimActivityReducer.class);
		    
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    
		    MultipleInputs.addInputPath(job1, new Path(cosSimTop), TextInputFormat.class, SimActivityMapper.class);
			MultipleInputs.addInputPath(job1, new Path(userActivity), TextInputFormat.class, SimDateMapper1.class);

		    FileOutputFormat.setOutputPath(job1, new Path(halfMerged));

		    boolean success = job1.waitForCompletion(true);
		    
		    @SuppressWarnings("deprecation")
			Job job2 = new Job();
		    job2.setJarByClass(SimActivity.class);
		    job2.setJobName("Merge sim values with user activity");

		    job2.setMapperClass(SimActivityMapper.class);
		    job2.setReducerClass(SimActivityRevReducer.class);
		    
		    job2.setMapOutputKeyClass(Text.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    
		    MultipleInputs.addInputPath(job2, new Path(halfMerged), TextInputFormat.class, SimDateMapperRev.class);
			MultipleInputs.addInputPath(job2, new Path(userActivity), TextInputFormat.class, SimDateMapper1.class);

		    FileOutputFormat.setOutputPath(job2, new Path(fullMerged));

		    success = job2.waitForCompletion(true);
		   
		    
		    
		    @SuppressWarnings("deprecation")
			Job job3 = new Job();
		    job3.setJarByClass(SimActivity.class);
		    job3.setJobName("sort Merged values");

	        FileInputFormat.addInputPath(job3, new Path(fullMerged));
		    FileOutputFormat.setOutputPath(job3, new Path(sortedMerged));
		

		    job3.setMapperClass(ResortMapper.class);
		    job3.setReducerClass(ResortReducer.class);
		    
		    job3.setMapOutputKeyClass(IntWritable.class);
		    job3.setMapOutputValueClass(Text.class);
		    job3.setOutputKeyClass(Text.class);
		    job3.setOutputValueClass(Text.class);

		    success = job3.waitForCompletion(true);
		    
		    System.exit(success ? 0 : 1);
	  }
}
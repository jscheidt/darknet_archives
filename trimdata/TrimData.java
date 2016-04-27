package trimdata;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import trimdata.TrimDatasetMapper.TrimDatasetMapperUserPostCount;

public class TrimData {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath = args[0];
		String inputPath1 = args[1];
		String outputPath = args[2];

		String flattenedGrams = inputPath;
		String UserPostCount = inputPath1;
		String TrimData = outputPath + "TrimData";

		// Configuration conf = new Configuration();

		Job job = new Job();
		job.setJarByClass(TrimData.class);
		job.setJobName("TrimData");
		job.setNumReduceTasks(10);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TrimDatasetMapper.class);
		job.setReducerClass(TrimDatasetReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(flattenedGrams), TextInputFormat.class, TrimDatasetMapper.class);
		MultipleInputs.addInputPath(job, new Path(UserPostCount), TextInputFormat.class, TrimDatasetMapperUserPostCount.class);

		FileOutputFormat.setOutputPath(job, new Path(TrimData));
		
		job.waitForCompletion(true);
	}
}

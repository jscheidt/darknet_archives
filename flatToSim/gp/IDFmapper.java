package gp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IDFmapper extends Mapper<LongWritable, Text, Text, Text> {
	// private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("<===>");
		Text author = new Text(split[0].trim());
		String actualValue = split[1].trim();

		String[] secondSplit = actualValue.split("<====>");
		Text word = new Text(secondSplit[0].trim());

		context.write(word, author);
	}

}
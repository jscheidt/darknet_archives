package gp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumAuthorsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("<===>");
		Text author = new Text(split[0].trim());

		context.write(one, author);
	}

}
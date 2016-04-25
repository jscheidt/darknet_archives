package gp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFmapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	//private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("<====>");
		Text newKey = new Text(split[0].toString().trim());
		DoubleWritable tfOrIDF = new DoubleWritable(Double.parseDouble(split[1].trim()));
		context.write(newKey, tfOrIDF);
	}

}

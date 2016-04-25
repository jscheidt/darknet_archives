package sumVectors;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumDotProductMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	//private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  	String[] split = value.toString().split("<====>");
			String users = split[0].trim();
			Double product = Double.valueOf(split[1].trim());
			
			context.write(new Text(users), new DoubleWritable(product));
	  }
	}
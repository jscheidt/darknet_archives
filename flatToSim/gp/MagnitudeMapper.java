package gp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MagnitudeMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	//private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  	String[] split = value.toString().split("<===>");
			String user = split[0].trim();
			
			String[] split2 = split[1].trim().split("<====>");
			Double tfXidf = Double.valueOf(split2[1].trim());
			
			context.write(new Text(user), new DoubleWritable(tfXidf));
	  }
	}
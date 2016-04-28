package gp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiplyMagnitudeMapper extends Mapper<Object, Text, IntWritable, Text>{

	private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  	
			
			context.write( one, value);
	  }
	}
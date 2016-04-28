package gp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopPercentMapper extends Mapper<Object, Text, IntWritable, Text>{

	private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  String[] split = value.toString().split("<====>");
			String users = split[0].trim();
			String similarity = split[1].trim();
			String product =  similarity + "<====>" + users;
			
			if(Double.parseDouble(similarity) >= 0.1)
				context.write( one, new Text(product));
	  }
	}
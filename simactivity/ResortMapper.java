
package simactivity;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ResortMapper extends Mapper<Object, Text, IntWritable, Text>{

	//this just resorts the top similarities because the reducers didn't take stuff
	//in order when merging the userActivity
	private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  
		  String[] split = value.toString().split("\t");
		  String[] realSplit = split[1].split("<====>");
		  String similarity = realSplit[0].trim();
		  String users = realSplit[1].trim();
		  
		  
		  String product =  similarity + "<====>" + users;
			
		  context.write( one, new Text(product));
	  }
	}
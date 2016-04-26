package userdate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeMapper extends Mapper<Object, Text, Text, Text>{

	//private final static IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  	String[] split = value.toString().split("<==>");
		  	String database = split[0].trim();
		  	String rest = split[1].trim();
			
			context.write(new Text(database), new Text(rest));
	  }
	}
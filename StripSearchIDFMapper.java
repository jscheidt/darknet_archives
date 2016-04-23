package gp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StripSearchIDFMapper extends Mapper<LongWritable, Text, Text, Text>{
	 //private final static IntWritable one = new IntWritable(1);
    
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
		   String[] split = value.toString().split("<===>");
		   String author = split[0].trim();
		   Text actualValue = new Text(split[1].trim());
	   
		   if(author.equalsIgnoreCase("search")){
			   context.write(new Text(author), actualValue);
		   }
	   }

	}
package gp;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NgramCountReducer
      extends Reducer<Text,IntWritable,Text,Text> {

	
   public void reduce(Text key, Iterable<IntWritable> values, Context context
		   ) throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
	    }
		
		String oldKey = key.toString();
		String[] split = oldKey.split("<===>");
		String databaseUser = split[0];
		String word = split[1];
		/*
		StringTokenizer spliter = new StringTokenizer(oldKey.toString(), "<=0=>");
		String databaseUser = spliter.nextToken();
		String word = spliter.nextToken();
		*/

		context.write( new Text(databaseUser), new Text("<===>" + word + "<====>" + sum));
	}
 }

package gp2;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetUserNamesReducer
      extends Reducer<Text,IntWritable,Text,Text> {

	
   public void reduce(Text key, Iterable<IntWritable> values, Context context
		   ) throws IOException, InterruptedException {
	   context.write( key, new Text(" "));
		
	}
 }

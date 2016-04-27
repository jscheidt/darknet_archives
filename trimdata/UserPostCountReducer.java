package usercount;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserPostCountReducer
      extends Reducer<Text,IntWritable,Text,Text> {

	
   public void reduce(Text key, Iterable<IntWritable> values, Context context
		   ) throws IOException, InterruptedException {


		int num = 0;
		for (IntWritable val : values) {
			num += val.get();
	    }
		
		if (num > 10){
			context.write( new Text(key), new Text(num + "<V>"));
		}
	}
 }
package userdate;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DBUserMinMaxReducer
      extends Reducer<Text,IntWritable,Text,Text> {

	
   public void reduce(Text key, Iterable<IntWritable> values, Context context
		   ) throws IOException, InterruptedException {

		int min = Integer.MAX_VALUE;
		int max = 0;
		int num;
		for (IntWritable val : values) {
			num = val.get();
//			System.out.println(key + " " + num);
			if (num < min){
				min = num;
			}
			if (num > max){
				max = num;
			}
	    }
//		System.out.println(key + "\t" + min + ":" + max);
		context.write( new Text(key), new Text(min + ":" + max));
	}
 }
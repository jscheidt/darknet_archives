
package simactivity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ResortReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
		
//		ArrayList<Double> doubList = new ArrayList<Double>();
//		Map<Double, String> map = new HashMap<Double, String>();
		
		ArrayList<String> temp = new ArrayList<String>();
		
		for( Text val : values )
		{
			temp.add(val.toString());
		}
		
		Collections.sort(temp);
		Collections.reverse(temp);
		
		for( int i = 0; i < temp.size(); i++)
		{
			context.write( one, new Text(temp.get(i)));
		}
	}
}
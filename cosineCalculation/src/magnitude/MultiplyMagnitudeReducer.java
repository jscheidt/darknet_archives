package magnitude;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyMagnitudeReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
		
		ArrayList<String> cache = new ArrayList<String>();
		ArrayList<Double> magnitude = new ArrayList<Double>();
		for( Text val: values)
		{
			String[] split = val.toString().split("<====>");
			String user = split[0].trim();
			Double mag = Double.valueOf(split[1].trim());
			cache.add(user);
			magnitude.add(mag);
		}
		
		for( int i = 0; i < cache.size(); i++){
	    	for( int k = i+1; k < cache.size(); k++){
	    		String user1 = cache.get(i);
	    		String user2 = cache.get(k);
	    		
	    		String[] user1Split = user1.split("<==>");
	    		String[] user2Split = user1.split("<==>");
	    		
	    		if( !user1Split[0].trim().equals(user2Split[0].trim()))
	    		{
	    			Double product = magnitude.get(i)*magnitude.get(k);
	    			context.write( new Text(user1 + "<=0=>" + user2 + "<====>M<00>"), new DoubleWritable(product));
	    			context.write( new Text(user2 + "<=0=>" + user1 + "<====>M<00>"), new DoubleWritable(product));
	    		}
	    	}
	    }
	}
}
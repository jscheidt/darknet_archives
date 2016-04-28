package gp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyMagnitudeReducer extends Reducer<IntWritable, Text, Text, DoubleWritable>{
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context)
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
	    		String[] user2Split = user2.split("<==>");
	    		String db1 = user1Split[0].trim();
	    		String db2 = user2Split[0].trim();
	    		
	    		if( !(db1.equals(db2)))
	    		{
	    			Double product = magnitude.get(i)*magnitude.get(k);
	    			if(db1.compareTo(db2)>0)
	    			{
	    				context.write( new Text(user1 + "<=0=>" + user2 + "<====>M<00>"), new DoubleWritable(product));
	    			}
	    			else
	    			{
	    				context.write( new Text(user2 + "<=0=>" + user1 + "<====>M<00>"), new DoubleWritable(product));
	    			}
	    		}
	    	}
	    }
	}
}
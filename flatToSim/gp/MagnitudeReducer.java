package gp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MagnitudeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException{
		
		double sum = 0.0;
		
		for( DoubleWritable val : values){
			sum += Math.pow(val.get(), 2);
		}
		
		context.write(new Text(key.toString().trim() + "<====>"), new DoubleWritable(Math.sqrt(sum)));
	}
}
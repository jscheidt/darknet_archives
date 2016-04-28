package gp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcSimilarityReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
		
		double dot = 0.0;
		double mag = 0.0;
		int count = 0;
		for( Text val : values){
			
			if( val.toString().contains("M<00>"))
			{
				String[] split = val.toString().trim().split("M<00>");
				mag = Double.valueOf(split[1].trim()).doubleValue();
			}
			else
			{
				dot = Double.valueOf(val.toString());
				count++;
			}
			
		}
		if( count == 1)
			context.write(new Text(key.toString().trim() + "<====>"), new DoubleWritable(dot/mag));
	}
}
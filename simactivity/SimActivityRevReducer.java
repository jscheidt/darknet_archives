package simactivity;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimActivityRevReducer extends Reducer<Text, Text, Text, Text>{
	//key = db<==>user
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
		
		ArrayList<String> cache = new ArrayList<String>();
		double similarity = 0.0;
		String userInfo = "";
		String user2 = "";
		int count = 0, count1 =0;
		for( Text val : values){
			
			if( val.toString().contains("<#>"))
			{
				cache.add(val.toString());
				count++;
			}
			else
			{
				userInfo = val.toString();
			
			}
		}
			if (count >=1){
			//1		0.9763888156119016<====>"Pandora<==>gonz324<=0=>"Onion Market<==>gonz324
			for (String vals : cache){
				String user1 = key.toString();
				String[] split = vals.toString().trim().split("<#>");
				similarity = Double.valueOf(split[0].trim()).doubleValue();
				user2 = split[1].trim();
				String output = similarity + "<====>" + user2 + "<=0=>" + user1 + "<===>" + userInfo;
				context.write(new Text("1"), new Text(output));
				
			}			
		}
	}
}
package userdate;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//DoubleWritable
//public class CalcSimilarityReducer extends Reducer<Text, Text, Text, DoubleWritable>{
public class MergeReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
		
		ArrayList<String> cache = new ArrayList<String>();

		String global="";
		for( Text val : values){
			
			//userMinMax
			if( val.toString().contains("<===>"))
			{
				cache.add(val.toString());
			}
			//globalMinMax
			else
			{
				global = val.toString();
				
			}
			
		}
		  for( int i = 0; i < cache.size(); i++){
		    		String[] info = cache.get(i).trim().split("<===>");
		    		String user = info[0].trim();
		    		String minMax = info[1].trim();
		    		

			context.write(new Text(key.toString().trim() + "<==>" 
					+ user + "<===>"), new Text(minMax + "<=G=>" + global));
		  }
	} 
}
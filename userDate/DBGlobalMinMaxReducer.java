package userdate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DBGlobalMinMaxReducer
      extends Reducer<Text,Text,Text,Text> {

	//recieves format from mapper as <DB, minVal:maxVal>
   public void reduce(Text key, Iterable<Text> values, Context context
		   ) throws IOException, InterruptedException {

		int globalMin = Integer.MAX_VALUE;
		int globalMax = 0;
		for (Text val : values) {
			String[] line = val.toString().split(":");
			String minVal = line[0].trim();
			String maxVal = line[1].trim();
			int min = Integer.parseInt(minVal);
			int max = Integer.parseInt(maxVal);
			if (min < globalMin){
				globalMin = min;
			}
			if (max > globalMax){
				globalMax = max;
			}

	    }
		//write out as "DB \t globalMin:globalMax"
		context.write(new Text(key), new Text(globalMin + ":" + globalMax));
	}
 }
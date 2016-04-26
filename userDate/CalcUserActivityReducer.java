package userdate;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcUserActivityReducer extends Reducer<Text, Text, Text, Text> {

	// recieves format from mapper as <DB<==>user,
	// minVal:maxVal<=G=>globMin:globMax>
	public void reduce(Text key, Iterable<Text> values, Context context
			   ) throws IOException, InterruptedException {
		
		int minVal=0, maxVal=0, globalMin=0, globalMax=0;
		int active=0, diffFirst=0, diffLast=0;
		for (Text val : values) {
			String[] line = val.toString().trim().split("<=G=>");
			String[] userVal = line[0].trim().split(":");
			String[] globalVal = line[1].trim().split(":");
			
			minVal = Integer.parseInt(userVal[0].trim());
			maxVal = Integer.parseInt(userVal[1].trim());
			globalMin = Integer.parseInt(globalVal[0].trim());
			globalMax = Integer.parseInt(globalVal[1].trim());
			
			active = maxVal - minVal;
			diffFirst = minVal - globalMin;
			diffLast = globalMax - maxVal;
		}    
			//write out as "DB \t globalMin:globalMax"
		String output = active + "<=A=>" + diffFirst + "<F=L>" + diffLast; 
		context.write(new Text(key), new Text(output));
	}

}

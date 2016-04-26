
package userdate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalcUserActivityMapper extends Mapper<Object, Text, Text, Text> {

	// this reads in from the file created in DBUserDateMapper/Reducer
	//the format is "DB<===>USER \t minVal:maxVal"
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().trim().split("<===>");
		String databaseUser = line[0].trim();
		String vals =	line[1].trim();
		
		context.write(new Text(databaseUser + "<===>"), new Text(vals));
		}
	
}
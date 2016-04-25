
package gp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBLastPostMapper extends Mapper<Object, Text, Text, Text> {

	// this reads in from the file created in DBUserDateMapper/Reducer
	//the format is "DB<===>USER \t minVal:maxVal"
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");
		String[] keyPart = line[0].split("<==>");
		String database = keyPart[0].trim();
//		String user = keyPart[1].trim();
		String vals =	line[1].trim();
		// System.out.println(database + "and " + vals);
		context.write(new Text(database), new Text(vals));
		}
	
}

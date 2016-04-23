
package gp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBUserDateMapper extends Mapper<Object, Text, Text, IntWritable>{


  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	    IntWritable intW;
		String[] line = value.toString().split("\",\"");
		String database = line[0].trim();
		database = database.replace("\"", "");
		String user = line[1].trim();
//		String description = line[2].trim();
		String timestamp = line[3].trim();
		timestamp = timestamp.replace("\",", "");
		System.out.println(timestamp);
//		StringTokenizer words = new StringTokenizer(description);
		try{
			int timestampInt = Integer.parseInt(timestamp);
			intW = new IntWritable(timestampInt);
			context.write(new Text(database + "<==>" + user), intW);
		}catch(Exception e){
			
		}
  }
}
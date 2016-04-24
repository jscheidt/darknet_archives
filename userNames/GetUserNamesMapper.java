package gp2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetUserNamesMapper extends Mapper<Object, Text, Text, IntWritable>{

private final static IntWritable one = new IntWritable(1);
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

		String[] line = value.toString().split("<===>");
		String databaseXuser = line[0].trim().toLowerCase();
		context.write(new Text(databaseXuser), one);
		//context.write(new Text(database + "<==>" + user), new Text(description + "<===>" + timestamp));
  	}
}
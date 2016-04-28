package gp;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RandomTrimMapper extends Mapper<Object, Text, Text, Text>{

//private final static IntWritable one = new IntWritable(1);
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  String[] split = value.toString().split("\",\"");
	  if( split.length != 4)
	  	  return;
	  String db = split[0].trim();
	  String user = split[1].trim();
	  String post = split[2].trim();
	  String time = split[3].trim();
	  
	  String dbXuser = db + "\",\"" + user + "\",\"";
	  String postXtime = post + "\",\"" + time;
	  context.write(new Text(dbXuser), new Text(postXtime));
}
}
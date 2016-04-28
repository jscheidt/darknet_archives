package gp;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CosineCountMapper extends Mapper<Object, Text, Text, Text>{

//private final static IntWritable one = new IntWritable(1);
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  String[] split = value.toString().split("<===>");
	  if( split.length != 2)
		  return;

      String DBxUser = split[0].trim();
	  
	  String[] split2 = split[1].split("<====>");
	  String word = split2[0].trim();
	  String TFxIDF = split2[1].trim();
	  
	  context.write(new Text(word), new Text(DBxUser + "<====>" + TFxIDF));
}
}
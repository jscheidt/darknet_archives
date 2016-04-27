package trimdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserPostCountMapper  extends Mapper<Object, Text, Text, IntWritable>{

	private static final IntWritable one = new IntWritable(1);
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  //takes input from flattenedgrams database aka (db, user, description, date)
			String[] line = value.toString().split("\",\"");
			if( line.length < 4)
				return;
			String database = line[0].trim();
			String user = line[1].trim();

			context.write(new Text(database + "<==>" + user + "<===>"), one);
	  }
	}
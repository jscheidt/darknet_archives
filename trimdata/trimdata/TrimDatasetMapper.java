package trimdata;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrimDatasetMapper extends Mapper<Object, Text, Text, Text>{

		  public void map(Object key, Text value, Context context)
		      throws IOException, InterruptedException {
			  //takes input from flattenedgrams database
			  //"market_name","vendor_name","description","add_time",
				String[] line = value.toString().split("\",\"");
				if( line.length < 4)
					return;
				String database = line[0].trim();
				String user = line[1].trim();
				String description = line[2].trim();
				String timestamp = line[3].trim();
				
				if (database.equals("\"market_name")){
					return;
				}
				else{
					context.write(new Text(database + "<==>" + user + "<===>"), new Text(description + "<====>" + timestamp));
		  }
		}

public static class TrimDatasetMapperUserPostCount extends Mapper<Object, Text, Text, Text>{

	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  //takes input from UserPostCountReducer 
		  //"The Pirate Market<==>xinhai<===>	18<V>
			String[] line = value.toString().trim().split("<===>");
			String databaseUser = line[0].trim();
			String postCount = line[1].trim();


			context.write(new Text(databaseUser + "<===>"), new Text(postCount));
	  }
	}
}
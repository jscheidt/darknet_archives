package trimdata;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrimDatasetReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<String> cache = new ArrayList<String>();
		int count = 0;

		for (Text val : values) {

			if (val.toString().contains("<V>")) {
//				context.write(new Text(key), new Text(val.toString()));
				// String[] split = val.toString().trim().split("<V>");
				count++;
			} else if (val.toString().contains("<====>")) {
//				context.write(new Text(key), new Text(val.toString()));
				cache.add(val.toString());

			}
		}
		
		
//		context.write(new Text(key), new Text(count + ""));

		if (count == 1) {
			for( int i = 0; i < cache.size(); i++){
				String val = cache.get(i);
//				context.write(new Text(val), new Text(""));

				String split1[] = val.toString().trim().split("<====>");
				String description = split1[0].trim();
				String timestamp = split1[1].trim();
				
				
//
//				String DBU = key.toString().replace("<===>", "");

				String splitDBU[] = key.toString().split("<==>");
				String database = splitDBU[0].trim();
				String user = splitDBU[1].trim();
				user = user.replace("<===>", "");
				
//				context.write(new Text(database), new Text(user));
//
				String line = database + "\",\"" + user + "\",\"" + description + "\",\"" + timestamp;

				context.write(new Text(line), new Text(""));
			}
		}

	}
}

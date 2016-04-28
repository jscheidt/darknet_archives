package gp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("\",\"");
		if (line.length < 4)
			return;
		String database = line[0].trim();
		String user = line[1].trim();
		String description = line[2].trim();
		// String timestamp = line[3].trim();
		StringTokenizer words = new StringTokenizer(description);
		while (words.hasMoreTokens()) {
			String cleaned = clean(words.nextToken());
			if (!cleaned.equals(""))
				context.write(new Text(database + "<==>" + user + "<===>" + cleaned), one);
		}
		// context.write(new Text(database + "<==>" + user), new
		// Text(description + "<===>" + timestamp));
	}

	// clean up the string for consumption
	private String clean(String nextToken) {
		return nextToken.toLowerCase();// .replaceAll("[^a-zA-Z]", "");
	}

}
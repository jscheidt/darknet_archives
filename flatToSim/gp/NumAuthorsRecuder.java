package gp;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumAuthorsRecuder extends Reducer<IntWritable, Text, IntWritable, Text> {

	Hashtable<String, Integer> authors = new Hashtable<String, Integer>();

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text val : values) {
			if (!authors.containsKey(val.toString()))
				authors.put(val.toString(), 1);
		}

		context.write(new IntWritable(authors.size()), new Text(" "));

		context.getCounter("Authors", "Authors").increment(authors.size());
	}
}
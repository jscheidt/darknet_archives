package gp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFreducer extends Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		HashMap<String, Integer> authors = new HashMap<String, Integer>();
		// authors.put("search", 1);

		double littleN = 0;
		for (Text val : values) {
			if (!authors.containsKey(val.toString())) {
				authors.put(val.toString(), 1);
			}
			littleN += 1.0;
		}

		Configuration conf = context.getConfiguration();
		long bigN = Integer.parseInt(conf.get("Authors"));

		double idf = Math.log(bigN / littleN) / Math.log(2.0);
		DoubleWritable IDF = new DoubleWritable(idf);
		Iterator<String> it = authors.keySet().iterator();
		while (it.hasNext()) {
			context.write(new Text(it.next() + "<===>" + key + "<====>"), IDF);
		}
	}
}

package gp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TermFrequencyReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	MultipleOutputs<Text, Text> multipleOutputs;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<String> cache = new ArrayList<String>();

		int max = 0;
		for (Text value : values) {

			String oldVal = value.toString();
			cache.add(oldVal);
			StringTokenizer spliter = new StringTokenizer(oldVal.toString(), "<====>");

			@SuppressWarnings("unused")
			String word = spliter.nextToken();
			String count = spliter.nextToken();

			int num = Integer.parseInt(count);
			if (num > max)
				max = num;

			// Text outVal = new Text("<===>" + key + "<====>" + num + "<==>" +
			// max);
			// context.write(new Text(word.trim()), outVal);
		}

		for (int i = 0; i < cache.size(); i++) {
			String[] split = cache.get(i).toString().split("<====>");
			/*
			 * for(Text val: cache){ //String oldVal = val.toString();
			 * //StringTokenizer spliter = new
			 * StringTokenizer(oldVal.toString(), "<==>"); String[] split =
			 * val.toString().split("<==>");
			 */
			String word = split[0].trim();
			String count = split[1];
			double num = Integer.parseInt(count);
			double frequency = (double) ((double) num / (double) max);

			// Text outVal = new Text("" + frequency);
			context.write(new Text(key.toString().trim() + "<===>" + word + "<====>"), new DoubleWritable(frequency));
			// context.write(key, new Text(cache.get(i)));
		}
	}
}

package gp;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFreducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
		throws IOException, InterruptedException{
		String[] split = key.toString().split("<===>");
		String author = split[0].trim();
		String word = split[1].trim();

		int count = 0;
		double tfidf = 1.0;
		for (DoubleWritable val : values) {
			tfidf  = tfidf * val.get();
			count++;
		}
		if( count < 2) {
			tfidf = 0.0;
		}
		context.write(new Text(author + "<===>" + word + "<====>"), new DoubleWritable(tfidf));
	}
}

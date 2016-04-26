package MarketSimilarity;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Takes as input a list of cosine similarities, ex. market1<===>user1<===>market2<====>user2    0.235235
//Takes average of all users from one market against all users from another market to find average similarity between markets

public class MarketSimilarity {

  public static class CosineMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String [] cosineSplit = value.toString().split("\\t");
	String [] marketSplit = cosineSplit[0].split("<===>");
	//market1<===>market2
	context.write(new Text(marketSplit[0] + "<===>" + marketSplit[2]), new Text(cosineSplit[1]));
    }
  }

  public static class CosineReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	double total = 0.0;
	double count = 0.0;
	for (Text v: values) {
		String [] cosineSplit = v.toString().split("\\t");
		total += Double.parseDouble(cosineSplit[1]);
		count += 1.0;
	}
	double avg = total / count;
	context.write(key, new Text(Double.toString(avg)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cosineAvg");
    job.setJarByClass(MarketSimilarity.class);
    job.setMapperClass(CosineMapper.class);
    //job.setCombinerClass(CosineReducer.class);
    job.setReducerClass(CosineReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //job.addCacheFile(new URI("/authorList#AuthorList"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

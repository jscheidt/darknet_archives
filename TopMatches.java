package gp;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.net.URI;
import java.util.Collections; 
import java.util.HashMap; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopMatches {

  public static class CosineMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	context.write(new Text("key"), new Text(value.toString().split("\\t")[1]));
    }
  }

  public static class CosineReducer extends Reducer<Text, Text, Text, Text> {
    Map<String, Double> topTenThousand = new HashMap<String, Double>();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	for (Text v: values) {
		String [] cosineSplit = v.toString().split("\\t");
		double cos = Double.parseDouble(cosineSplit[1]);
		String match = cosineSplit[0];
		topTenThousand.put(match, cos);
		//context.write(key, v);
	}
	
	//context.write(key, new Text(Double.toString(total/c)));

	Map<String, Double> sorted = sortByValues(topTenThousand);
	int check = sorted.size() < 10000 ? sorted.size() : 10000;
	Iterator it = sorted.entrySet().iterator();
	int count = 0;
	while (it.hasNext() && count < 10000) {
		Map.Entry pair = (Map.Entry)it.next();
		double d = (double) pair.getValue();
		context.write(new Text(), new Text(pair.getKey() + "\\t" + Double.toString(d)));
		count++;
	}
    }
  }

//function obtained from http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
  public static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
    Comparator<K> valueComparator =  new Comparator<K>() {
        public int compare(K k1, K k2) {
            int compare = map.get(k2).compareTo(map.get(k1));
            if (compare == 0) return 1;
            else return compare;
        }
    };
    Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
    sortedByValues.putAll(map);
    return sortedByValues;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cosineAvg");
    job.setJarByClass(TopMatches.class);
    job.setMapperClass(CosineMapper.class);
    job.setCombinerClass(CosineReducer.class);
    job.setReducerClass(CosineReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //job.addCacheFile(new URI("/authorList#AuthorList"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

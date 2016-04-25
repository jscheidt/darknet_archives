package gp;

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

public class Cosine {

  public static class CosineMapper extends Mapper<Object, Text, Text, Text>{
    ArrayList<String> authorList = new ArrayList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

      if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
        File file = new File("./AuthorList");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
          String line;
          while ((line = br.readLine()) != null) {
            authorList.add(line);
          }
        }
      }
      super.setup(context);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	for (int i = 0; i < authorList.size(); i++) {
		String [] authorSplit = value.toString().split("\\t");
		Text match1 = new Text(authorList.get(i) + "<===>" + authorSplit[0]);
		Text match2 = new Text(authorSplit[0] + "<===>" + authorList.get(i));
		Text vector = new Text(authorSplit[1]);
		context.write(match1, vector);
		context.write(match2, vector);
	}
    }
  }

  public static class CosineReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	ArrayList<Double> vector1 = new ArrayList<Double>();
	ArrayList<Double> vector2 = new ArrayList<Double>();
	v_count = 0;
	for (Text v: values) {
		String [] authorSplit = value.toString().split("\\t");
		String spl = authorSplit[1].replace("[", "");
		spl = spl.replace("]", "");
		String [] lineSplit = spl.split(", ");
		if (v_count == 0) {
			for (int i = 0; i < lineSplit.length - 1; i++) {
				vector1.add(Double.parseDouble(lineSplit[i]));
			}
		}else {
			for (int i = 0; i < lineSplit.length - 1; i++) {
				vector2.add(Double.parseDouble(lineSplit[i]));
			}
		}
	}

	double num = 0.0;
	double denA = 0.0;
	double denB = 0.0;
	for (int i = 0; i < lineSplit.length - 1; i++) {
		num += vector1.get(i) * vector2.get(i);
		denA += vector1.get(i) * vector1.get(i);
		denB += vector2.get(i) * vector2.get(i);
	}
	double cosine = num / (Math.sqrt(denA) * Math.sqrt(denB));
	context.write(key, new Text(Double.toString(cosine)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "cosine");
    job.setJarByClass(Cosine.class);
    job.setMapperClass(CosineMapper.class);
    //job.setCombinerClass(CosineReducer.class);
    job.setReducerClass(CosineReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.addCacheFile(new URI("/authorList#AuthorList"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

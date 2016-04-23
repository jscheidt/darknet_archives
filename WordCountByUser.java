package p2offline;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
Job 1: Word Frequency for Author
Mapper
Input: (author, contents)
Output: ((word, author), 1)
Reducer
Sums counts for word in document
Outputs ((word, author), n)
Combiner is same as Reducer

Job 2: Word Counts for Authors
Mapper
Input: ((word, author), n)
Output: (author, (word, n)) 
Reducer
Sums frequency of individual n’s in same doc
Feeds original data through
Outputs ((word, author), (n, N))

Job 3: Word Frequency in Corpus
Mapper
Input: ((word, author), (n, N))
Output: (word, (author, n, N, 1))
Reducer
Sums counts for word in corpus
Outputs ((word, author), (n, N, m))

Job 4: Calculate TF-IDF
Mapper
Input: ((word, author), (n, N, m))
Assume D is known (or, easy MR to find it)
Output ((word, author), TF*IDF)
Reducer
Just the identity function

*/

public class WordFreqAuthor {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    //variables where output will be written to
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    //gets rid of anything that isn't a letter or a space
    private static final String regex = "[^A-Za-z ]";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String [] line = value.toString().split("<===>");
      String author = line[0];
      String prunedText = line[1].replaceAll(regex, "").toLowerCase();
      StringTokenizer itr = new StringTokenizer(prunedText);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken() + "@" + author);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable count = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable c : values) {
        sum += c.get(); 
      }
      count.set(sum);
      context.write(key, count);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word freq author");
    job.setJarByClass(WordFreqAuthor.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

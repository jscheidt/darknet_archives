package gp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCounter {

	  public static void main(String[] args) throws Exception {

	    //int numReduceTasks = 50;
	    
	    String inputPath = args[0];
	    String outputPath = args[1];
	    String job1OutputPath = args[1]+"job01";

	    @SuppressWarnings("deprecation")
		Job job1 = new Job();
	    job1.setJarByClass(NgramCounter.class);
	    job1.setJobName("compress Volumes");

        FileInputFormat.addInputPath(job1, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
	    
	    //job1.setInputFormatClass(WholeFileInputFormat.class);
	    
	    //job1.setNumReduceTasks(numReduceTasks);

	    job1.setMapperClass(NgramCountMapper.class);
	    job1.setCombinerClass(NgramCountCombiner.class);
	    job1.setReducerClass(NgramCountReducer.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(IntWritable.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);

	    boolean success = job1.waitForCompletion(true);
	    
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	    @SuppressWarnings("deprecation")
		Job termFrequency = new Job();
	    termFrequency.setJarByClass(NgramCounter.class);
	    termFrequency.setJobName("get tf");
	    
	    String termFrequencyOutput = outputPath + "TF";

        FileInputFormat.addInputPath(termFrequency, new Path(job1OutputPath));
	    FileOutputFormat.setOutputPath(termFrequency, new Path(termFrequencyOutput));
	    
	    termFrequency.setMapperClass(TermFrequencyMapper.class);
	    termFrequency.setReducerClass(TermFrequencyReducer.class);
	    
	    termFrequency.setMapOutputKeyClass(Text.class);
	    termFrequency.setMapOutputValueClass(Text.class);
	    termFrequency.setOutputKeyClass(Text.class);
	    termFrequency.setOutputValueClass(DoubleWritable.class);
	    
	    success = termFrequency.waitForCompletion(true);
	    
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    

	    
	    @SuppressWarnings("deprecation")
		Job getNumAuthors = new Job();
	    getNumAuthors.setJarByClass(NgramCounter.class);
	    getNumAuthors.setJobName("get number of authors");
	    
	    String numAuthorsOutput = outputPath + "numAuthors";
	    
	    FileInputFormat.addInputPath(getNumAuthors, new Path(job1OutputPath));
	    FileOutputFormat.setOutputPath(getNumAuthors, new Path(numAuthorsOutput));
	    
	    getNumAuthors.setMapperClass(NumAuthorsMapper.class);
	    getNumAuthors.setReducerClass(NumAuthorsRecuder.class);
	    
	    getNumAuthors.setMapOutputKeyClass(IntWritable.class);
	    getNumAuthors.setMapOutputValueClass(Text.class);
	    getNumAuthors.setOutputKeyClass(IntWritable.class);
	    getNumAuthors.setOutputValueClass(Text.class);
	    
	    getNumAuthors.waitForCompletion(true);
	    
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	    long numAuthors = 10;

	    numAuthors = getNumAuthors.getCounters().findCounter("Authors","Authors").getValue();
	    
	    Configuration conf = new Configuration();
	    conf.setLong("Authors", numAuthors);
	     
	    @SuppressWarnings("deprecation")
		Job invertedDocumentFrequency = new Job(conf);
	    invertedDocumentFrequency.setJarByClass(NgramCounter.class);
	    invertedDocumentFrequency.setJobName("get IDF");

		String IDFoutput = outputPath + "IDF";

        FileInputFormat.addInputPath(invertedDocumentFrequency, new Path(job1OutputPath));
	    FileOutputFormat.setOutputPath(invertedDocumentFrequency, new Path(IDFoutput));
	    
	    invertedDocumentFrequency.setMapperClass(IDFmapper.class);
	    invertedDocumentFrequency.setReducerClass(IDFreducer.class);
	    
	    invertedDocumentFrequency.setMapOutputKeyClass(Text.class);
	    invertedDocumentFrequency.setMapOutputValueClass(Text.class);
	    invertedDocumentFrequency.setOutputKeyClass(Text.class);
	    invertedDocumentFrequency.setOutputValueClass(DoubleWritable.class);
	    
	    invertedDocumentFrequency.waitForCompletion(true);
	    
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    
	    @SuppressWarnings("deprecation")
		Job tfIDF = new Job(conf);
	    tfIDF.setJarByClass(NgramCounter.class);
	    tfIDF.setJobName("get IDF");
	    
	    @SuppressWarnings("unused")
		String tfIDFpath = outputPath + "tfIDF";

        FileInputFormat.addInputPath(tfIDF, new Path(IDFoutput));
        FileInputFormat.addInputPath(tfIDF, new Path(termFrequencyOutput));
	    FileOutputFormat.setOutputPath(tfIDF, new Path(outputPath));
	    
	    tfIDF.setMapperClass(TFIDFmapper.class);
	    tfIDF.setReducerClass(TFIDFreducer.class);
	    
	    tfIDF.setMapOutputKeyClass(Text.class);
	    tfIDF.setMapOutputValueClass(DoubleWritable.class);
	    tfIDF.setOutputKeyClass(Text.class);
	    tfIDF.setOutputValueClass(DoubleWritable.class);
	    
	    success = tfIDF.waitForCompletion(true);
	    
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    /*
	    @SuppressWarnings("deprecation")
		Job getSearchIDF = new Job();
	    getSearchIDF.setJarByClass(NgramCounter.class);
	    getSearchIDF.setJobName("get only idf with author of search");
	    
	    String searchIDFoutput = outputPath + "searchIDF";
	    
	    FileInputFormat.addInputPath(getSearchIDF, new Path(IDFoutput));
	    FileOutputFormat.setOutputPath(getSearchIDF, new Path(searchIDFoutput));
	    
	    getSearchIDF.setMapperClass(StripSearchIDFMapper.class);
	    getSearchIDF.setReducerClass(StripSearchIDFReducer.class);
	    
	    getSearchIDF.setMapOutputKeyClass(Text.class);
	    getSearchIDF.setMapOutputValueClass(Text.class);
	    getSearchIDF.setOutputKeyClass(Text.class);
	    getSearchIDF.setOutputValueClass(Text.class);
	    
	    getSearchIDF.waitForCompletion(true);
	    */
	    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	    
	    System.exit(success ? 0 : 1);
	  }
}

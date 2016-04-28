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

		// int numReduceTasks = 50;

		String inputPath = args[0];
		String outputPath = args[1];
		
	    String job1OutputPath = outputPath+"randomTrim";
	    
	    @SuppressWarnings("deprecation")
		Job job1 = new Job();
	    job1.setJarByClass(RandomTrim.class);
	    job1.setJobName("randomly removes 3/4 users");
	    job1.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));
	

	    job1.setMapperClass(RandomTrimMapper.class);
	    job1.setReducerClass(RandomTrimReducer.class);
	    
	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    boolean success = job1.waitForCompletion(true);
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		String ngramPath = args[1] + "job01";

		@SuppressWarnings("deprecation")
		Job ngramCounter = new Job();
		ngramCounter.setJarByClass(NgramCounter.class);
		ngramCounter.setJobName("compress Volumes");

		FileInputFormat.addInputPath(ngramCounter, new Path(job1OutputPath));
		FileOutputFormat.setOutputPath(ngramCounter, new Path(ngramPath));

		// job1.setInputFormatClass(WholeFileInputFormat.class);

		// job1.setNumReduceTasks(numReduceTasks);

		ngramCounter.setMapperClass(NgramCountMapper.class);
		ngramCounter.setReducerClass(NgramCountReducer.class);

		ngramCounter.setMapOutputKeyClass(Text.class);
		ngramCounter.setMapOutputValueClass(IntWritable.class);
		ngramCounter.setOutputKeyClass(Text.class);
		ngramCounter.setOutputValueClass(IntWritable.class);

		success = ngramCounter.waitForCompletion(true);

		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		@SuppressWarnings("deprecation")
		Job termFrequency = new Job();
		termFrequency.setJarByClass(NgramCounter.class);
		termFrequency.setJobName("get tf");

		String termFrequencyOutput = outputPath + "TF";

		FileInputFormat.addInputPath(termFrequency, new Path(ngramPath));
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

		FileInputFormat.addInputPath(getNumAuthors, new Path(ngramPath));
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

		numAuthors = getNumAuthors.getCounters().findCounter("Authors", "Authors").getValue();

		Configuration conf = new Configuration();
		conf.setLong("Authors", numAuthors);

		@SuppressWarnings("deprecation")
		Job invertedDocumentFrequency = new Job(conf);
		invertedDocumentFrequency.setJarByClass(NgramCounter.class);
		invertedDocumentFrequency.setJobName("get IDF");

		String IDFoutput = outputPath + "IDF";

		FileInputFormat.addInputPath(invertedDocumentFrequency, new Path(ngramPath));
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

		@SuppressWarnings("deprecation")
		Job cosineCalc = new Job(conf);
		cosineCalc.setJarByClass(NgramCounter.class);
		cosineCalc.setJobName("calculate dot product");

		String cosinePath = outputPath + "cosineCalc";

		FileInputFormat.addInputPath(cosineCalc, new Path(outputPath));
		FileOutputFormat.setOutputPath(cosineCalc, new Path(cosinePath));

		cosineCalc.setMapperClass(CosineCountMapper.class);
		cosineCalc.setReducerClass(CosineCountReducer.class);

		cosineCalc.setMapOutputKeyClass(Text.class);
		cosineCalc.setMapOutputValueClass(Text.class);
		cosineCalc.setOutputKeyClass(Text.class);
		cosineCalc.setOutputValueClass(DoubleWritable.class);
		
		success = cosineCalc.waitForCompletion(true);
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		@SuppressWarnings("deprecation")
		Job sumDotProduct = new Job(conf);
		sumDotProduct.setJarByClass(NgramCounter.class);
		sumDotProduct.setJobName("sum dot products");
		
		String dotProductPath = outputPath + "sumDotProduct";
		
		FileInputFormat.addInputPath(sumDotProduct, new Path(cosinePath));
		FileOutputFormat.setOutputPath(sumDotProduct, new Path(dotProductPath));
		
		sumDotProduct.setMapperClass(SumDotProductMapper.class);
		sumDotProduct.setReducerClass(SumDotProductReducer.class);
		
		sumDotProduct.setMapOutputKeyClass(Text.class);
		sumDotProduct.setMapOutputValueClass(DoubleWritable.class);
		sumDotProduct.setOutputKeyClass(Text.class);
		sumDotProduct.setOutputValueClass(DoubleWritable.class);
		
		success = sumDotProduct.waitForCompletion(true);
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		@SuppressWarnings("deprecation")
		Job magnitude = new Job(conf);
		magnitude.setJarByClass(NgramCounter.class);
		magnitude.setJobName("calculate individual magnitude");
		
		String magnitudePath = outputPath + "magnitude";
		
		FileInputFormat.addInputPath(magnitude, new Path(outputPath));
		FileOutputFormat.setOutputPath(magnitude, new Path(magnitudePath));
		
		magnitude.setMapperClass(MagnitudeMapper.class);
		magnitude.setReducerClass(MagnitudeReducer.class);
		
		magnitude.setMapOutputKeyClass(Text.class);
		magnitude.setMapOutputValueClass(DoubleWritable.class);
		magnitude.setOutputKeyClass(Text.class);
		magnitude.setOutputValueClass(DoubleWritable.class);
		
		success = magnitude.waitForCompletion(true);
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	    @SuppressWarnings("deprecation")
		Job magnitudeMultiply = new Job();
	    magnitudeMultiply.setJarByClass(NgramCounter.class);
	    magnitudeMultiply.setJobName("multiplyMagnitude");
	    
	    String magMultPath = outputPath+"multiplyMag";

        FileInputFormat.addInputPath(magnitudeMultiply, new Path(magnitudePath));
	    FileOutputFormat.setOutputPath(magnitudeMultiply, new Path(magMultPath));
	

	    magnitudeMultiply.setMapperClass(MultiplyMagnitudeMapper.class);
	    magnitudeMultiply.setReducerClass(MultiplyMagnitudeReducer.class);
	    
	    magnitudeMultiply.setMapOutputKeyClass(IntWritable.class);
	    magnitudeMultiply.setMapOutputValueClass(Text.class);
	    magnitudeMultiply.setOutputKeyClass(Text.class);
	    magnitudeMultiply.setOutputValueClass(DoubleWritable.class);
	    
	    success = magnitudeMultiply.waitForCompletion(true);
	    
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	    @SuppressWarnings("deprecation")
		Job calcSimilarity = new Job();
	    calcSimilarity.setJarByClass(NgramCounter.class);
	    calcSimilarity.setJobName("multiplyMagnitude");
	    
	    String similarityPath = outputPath+"similarity";

        FileInputFormat.addInputPath(calcSimilarity, new Path(magMultPath));
        FileInputFormat.addInputPath(calcSimilarity, new Path(dotProductPath));
	    FileOutputFormat.setOutputPath(calcSimilarity, new Path(similarityPath));
	

	    calcSimilarity.setMapperClass(CalcSimilarityMapper.class);
	    calcSimilarity.setReducerClass(CalcSimilarityReducer.class);
	    
	    calcSimilarity.setMapOutputKeyClass(Text.class);
	    calcSimilarity.setMapOutputValueClass(Text.class);
	    calcSimilarity.setOutputKeyClass(Text.class);
	    calcSimilarity.setOutputValueClass(DoubleWritable.class);
	    
	    success = calcSimilarity.waitForCompletion(true);
	    
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	    @SuppressWarnings("deprecation")
		Job top5000 = new Job();
	    top5000.setJarByClass(NgramCounter.class);
	    top5000.setJobName("multiplyMagnitude");
	    
	    String topPath = outputPath+"top";

        FileInputFormat.addInputPath(top5000, new Path(similarityPath));
	    FileOutputFormat.setOutputPath(top5000, new Path(topPath));
	

	    top5000.setMapperClass(TopPercentMapper.class);
	    top5000.setReducerClass(TopPercentReducer.class);
	    
	    top5000.setMapOutputKeyClass(IntWritable.class);
	    top5000.setMapOutputValueClass(Text.class);
	    top5000.setOutputKeyClass(Text.class);
	    top5000.setOutputValueClass(Text.class);

	    
	    success = top5000.waitForCompletion(true);

		System.exit(success ? 0 : 1);
	}
}

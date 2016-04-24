package co;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CosineCountReducer
      extends Reducer<Text,Text,Text,DoubleWritable> {

	
   public void reduce(Text key, Iterable<Text> values, Context context
		   ) throws IOException, InterruptedException {

	    ArrayList<String> cache = new ArrayList<String>();
	    ArrayList<Double> tfXidf = new ArrayList<Double>(); 
	    for( Text val : values){
	    	String[] split = val.toString().trim().split("<====>");
	    	String dbXuser = split[0].trim();
	    	Double tfIdf = Double.valueOf(split[1].trim());
	    	cache.add(dbXuser);
	    	tfXidf.add(tfIdf);
	    }
	    
	    for( int i = 0; i < cache.size(); i++){
	    	for( int k = i+1; k < cache.size(); k++){
	    		String user1 = cache.get(i);
	    		String user2 = cache.get(k);
	    		
	    		context.write( new Text(user1 + "<=0=>" + user2 + "<====>"), new DoubleWritable(tfXidf.get(i)*tfXidf.get(k)));
	    	}
	    }
	}
 }

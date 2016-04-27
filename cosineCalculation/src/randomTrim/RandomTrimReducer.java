package randomTrim;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RandomTrimReducer
      extends Reducer<Text,Text,Text,Text> {

	
	
   public void reduce(Text key, Iterable<Text> values, Context context
		   ) throws IOException, InterruptedException {
	   
	   Random rand = new Random();

	   int randomBucket = rand.nextInt(4);
	    if( randomBucket == 1)
	    {
		    for( Text val: values){
		    	context.write(key, val);
		    }
	    }
	}
 }


package simactivity;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimActivityMapper extends Mapper<Object, Text, Text, Text>{

	//reads in from top similarity list
	//	1	0.9763888156119016<====>"Pandora<==>gonz324<=0=>"Onion Market<==>gonz324

	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  String[] split = value.toString().split("\t");
		  String[] realSplit = split[1].split("<====>");
		  String similarity = realSplit[0].trim();
		  String[] users = realSplit[1].split("<=0=>");
		  String user1 = users[0].trim();
		  String user2 = users[1].trim();

		  context.write(new Text(user1), new Text(similarity + "<#>" + user2));
//		  context.write(new Text(user2 + "<===>"), new Text(similarity + "<#>"));
	  }
	}

class SimDateMapperRev extends Mapper<Object, Text, Text, Text>{

	//reads in from top similarity list
	//	1	0.9763888156119016<====>"Pandora<==>gonz324<=0=>"Onion Market<==>gonz324

	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  String[] split = value.toString().split("\t");
		  String[] realSplit = split[1].split("<====>");
		  String similarity = realSplit[0].trim();
		  String[] users = realSplit[1].split("<=0=>");
		  String user1 = users[0].trim();
		  String user2 = users[1].trim();

		  context.write(new Text(user2), new Text(similarity + "<#>" + user1));
//		  context.write(new Text(user2 + "<===>"), new Text(similarity + "<#>"));
	  }
	}

class SimDateMapper1 extends Mapper<Object, Text, Text, Text>{

	//reads in from userActivity file
	//"Abraxas<==>FreeTrade<===>	2039390<=A=>3798160<F=L>61362
	
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
		  String[] split = value.toString().split("<===>");	
		  String dbUsers = split[0].trim();
		  String info = split[1].trim();

		  context.write(new Text(dbUsers), new Text(info));
	  }
	}
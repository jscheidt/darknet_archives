package analysisOfUsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Analysis {

	public Analysis() {

	}

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		Analysis a = new Analysis();
//		a.readTextFile("finalSortedData1");
//		a.readTextFile1("finalChain-Trim1");
//		a.removeDuplicateUsers("finalSortedData-Trim.txt");
//		a.sortChainsByBiggest("userChain.txt");
		a.countUniqueUsers("sortedChainsByNumOfUsers");
	}
	
	protected void countUniqueUsers(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		String line = null;
		FileReader fr;
		PrintWriter writer = new PrintWriter("countNumOfUniqueUsersPerChain", "UTF-8");
		PrintWriter writer1 = new PrintWriter("sameUserAcrossDifferentDBs", "UTF-8");
		PrintWriter writer2 = new PrintWriter("sameUserAcrossDifferentDBswithDBs", "UTF-8");
		
		try {
			fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);

			while ((line = br.readLine()) != null) {
				int countNum = 0;
				String splitCount[] = line.split(":  ");
				ArrayList<String> usersPerChain = new ArrayList<String>(); 
				String newLine = splitCount[1].replace("{", "");
				newLine = newLine.replace("}", "");
//				System.out.println(newLine);
				String[] firstSplit = newLine.trim().split(", ");
				
				for (String val: firstSplit){
					String[] split = val.trim().split("\"=>\"");
					String similarity = split[1].trim().replace("\"", "");

					String[] users = split[0].split("<=0=>");
					

					String[] user1Activity = users[0].split("<=A=>");
					String[] user2Activity = users[1].split("<=A=>");
					
					String[] user1GetData = user1Activity[0].split("<===>");
					String[] user2GetData = user2Activity[0].split("<===>");
					
					String dbUser1 = user1GetData[0].replace("\"", "").replace("<==>", "-");
					String dbUser2 = user2GetData[0].replace("\"", "").replace("<==>", "-");
//					System.out.println(dbUser1);
//					System.out.println(dbUser2);
					if (!usersPerChain.contains(dbUser1)){
						usersPerChain.add(dbUser1);
					}
					if (!usersPerChain.contains(dbUser2)){
						usersPerChain.add(dbUser2);
					}

//					writer.println(newLine);

				}
				ArrayList<String> uniqueUserName = new ArrayList<String>();
				Map <String, ArrayList<String>> userDbMap = new HashMap<String, ArrayList<String>>();
				writer.print("UniqueUsers=" + usersPerChain.size() + ": ");
				//get uniqueusernames
				for (String user : usersPerChain){
					String split[] = user.split("-");
					if (!uniqueUserName.contains(split[1])){
						uniqueUserName.add(split[1]);
					}
				}
				//get the database for each user
				for (String uniqU : uniqueUserName){
					ArrayList<String> dbPerUser = new ArrayList<String>();
					for (String user : usersPerChain){
						String split[] = user.split("-");
						String db = split[0];
						String username = split[1];
						if (uniqU.equals(username)){
							if (!dbPerUser.contains(db)){
								dbPerUser.add(db);
							}
						}
						
					}
					userDbMap.put(uniqU, dbPerUser);
				}
				
				Set set = userDbMap.entrySet();
			      Iterator iterator = set.iterator();
			      
			      if (uniqueUserName.size() > 5){
			      writer2.print("NumOfSameUsers=" +uniqueUserName.size() +": ");
			      while(iterator.hasNext()) {
			         Map.Entry mentry = (Map.Entry)iterator.next();
			         System.out.print(mentry.getKey() + " " +mentry.getValue() + ", ");
			         writer2.print(mentry.getKey() + " " +mentry.getValue() + ", "); 
			      }
			      writer2.println();
			      }
				
				writer1.print("NumOfSameUsers=" +uniqueUserName.size() +": ");
				for (String uniqU : uniqueUserName){
//					System.out.print(uniqU + ", ");
					writer1.print(uniqU + ", ");
				}
				writer1.println();
				System.out.println();
					for (String user : usersPerChain){
//						System.out.println(user);
						writer.print(user + ", ");
					}
				writer.println();
			}
			writer.close();
			writer1.close();
			writer2.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
	
	protected void sortChainsByBiggest(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		int count = 1;
		
		String line = null;
		FileReader fr;
		PrintWriter writer = new PrintWriter("sortedChainsByNumOfUsers", "UTF-8");
		ArrayList<String> cache = new ArrayList<String>();
		
		try {
			fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);

			while ((line = br.readLine()) != null) {
				int countNum = 0;
//				String newLine = line.replace("{", "");
//				newLine = line.replace("}", "");

				String[] firstSplit = line.trim().split(", ");
				String newLine = (firstSplit.length * .1)  + ": " + line;
//				System.out.println(firstSplit.length + ": " + line);
				cache.add(newLine);
			}
			Collections.sort(cache);
			Collections.reverse(cache);
			for (String val : cache){
				String split[] = val.split(":");
				double num = Double.parseDouble(split[0]) * 10;
				int num1 = (int) num;
//				int num = Integer.parseInt(split[0])  * 10;
				System.out.println(num1 + ": " + split[1]);
				writer.println(num1 + ": " + split[1]);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
	
	protected void removeDuplicateUsers(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		int count = 1;
		String line = null;
		FileReader fr;
		PrintWriter writer = new PrintWriter("dataWithNoSameUsers", "UTF-8");
		
		try {
			fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);

			while ((line = br.readLine()) != null) {
				String[] split = line.toString().split("\t");
				String[] realSplit = split[1].split("<====>");
				String similarity = realSplit[0].trim();
				String[] users = realSplit[1].trim().split("<=0=>");
//				System.out.println(users[0]);

				String[] user1GetData = users[0].split("<===>");
				String[] user2GetData = users[1].split("<===>");
				
				String[] dbUser1 = user1GetData[0].split("<==>");
				String[] dbUser2 = user2GetData[0].split("<==>");
				
				String db1 = dbUser1[0];
				String db2 = dbUser2[0];
				String user1 = dbUser1[1].toLowerCase();
				String user2 = dbUser2[1].toLowerCase();
//				System.out.println(user1 + " vs " + user2);
				
				if (user1.equals(user2)){
					continue;
				}
				System.out.println(line);
				
				String[] user1Activity = user1GetData[1].split("<=A=>");
				String[] user2Activity = user2GetData[1].split("<=A=>");
//				System.out.println(line);
				
				if (user1Activity[0].equals("0") || user2Activity[0].equals("0"))
					continue;
				writer.println(line);

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

	protected void readTextFile(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		int count = 1;
		String line = null;
		FileReader fr;
		PrintWriter writer = new PrintWriter("finalSortedData-Trim", "UTF-8");
		
		try {
			fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);

			while ((line = br.readLine()) != null) {
				if (line.contains("vendor_name")){
					continue;
				}
				String[] split = line.toString().split("\t");
				String[] realSplit = split[1].split("<====>");
				String similarity = realSplit[0].trim();
				String[] users = realSplit[1].trim().split("<=0=>");

				String[] user1GetData = users[0].split("<===>");
				String[] user2GetData = users[1].split("<===>");
				
				String[] user1Activity = user1GetData[1].split("<=A=>");
				String[] user2Activity = user2GetData[1].split("<=A=>");
				
				
				if (user1Activity[0].equals("0") || user2Activity[0].equals("0"))
					continue;
				writer.println(line);

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
	
	protected void readTextFile1(String filename) throws FileNotFoundException, UnsupportedEncodingException {
		int count = 1;
		
		String line = null;
		FileReader fr;
		PrintWriter writer = new PrintWriter("analysis2", "UTF-8");
		
		try {
			fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);
			int seconds = 604800; //2 weeks

			while ((line = br.readLine()) != null) {
				int countNum = 0;
				String newLine = line.replace("{", "");
				newLine = line.replace("}", "");

				String[] firstSplit = newLine.trim().split(", ");
				boolean withinRange = false;
				for (String val: firstSplit){
					String[] split = val.trim().split("\"=>\"");
					String similarity = split[1].trim().replace("\"", "");

					String[] users = split[0].split("<=0=>");

					String[] user1Activity = users[0].split("<=A=>");
					String[] user2Activity = users[1].split("<=A=>");
					String[] firstLast1 = user1Activity[1].split("<F=L>");
					String[] firstLast2 = user2Activity[1].split("<F=L>");

					int firstUser1 = Integer.parseInt(firstLast1[0]);
					int firstUser2 = Integer.parseInt(firstLast2[0]);
					int lastUser1 = Integer.parseInt(firstLast1[1]);
					int lastUser2 = Integer.parseInt(firstLast2[1]);
					
					if (firstUser1 < seconds || firstUser2 < seconds || lastUser1 < seconds || lastUser2 < seconds ){
						withinRange = true;
						countNum++;
					}
//					if (firstUser2 < seconds && lastUser1 < seconds){
//						countNum++;
//						withinRange = true;
//					}
				
				}
				if (withinRange){
					writer.println(newLine);
					System.out.println(count + ":" + newLine);
					count++;
				}

//				if (countNum >= firstSplit.length){
//					writer.println(newLine);
//					System.out.println(count + ":" + newLine);
//					count++;
//				}

			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
}

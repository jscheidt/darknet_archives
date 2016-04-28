package removeactivity;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class RemoveActivity {

	public RemoveActivity() {

	}

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		RemoveActivity ra = new RemoveActivity();
//		ra.readTextFile("finalSortedData1");
		ra.readTextFile1("finalChain-Trim1");
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

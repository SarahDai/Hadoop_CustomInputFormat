import java.io.*;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang.RandomStringUtils;

public class CreateTask2Dataset {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		PrintWriter writer = new PrintWriter("CustomerJson.txt", "UTF-8");
		String[] gender = {"male","female"};
		
		for(int i=1; i<=400000; i++){
			writer.println("{ Customer ID:"+i+",");
			writer.println("Name:"+RandomStringUtils.randomAlphabetic(100)+",");
			writer.println("Address:"+RandomStringUtils.randomAlphabetic(100)+",");
			writer.println("Salary:"+ThreadLocalRandom.current().nextInt(100,1001)+",");
			writer.println("Gender:"+gender[ThreadLocalRandom.current().nextInt(2)]);
			if(i==400000)
				writer.print("}");
			else
				writer.println("},");
		}
		
		writer.close();

	}

}

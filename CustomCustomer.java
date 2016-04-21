import java.io.IOException;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import CustomInputFormat.*;

/*class CustomLineRecordReader extends LineRecordReader {
	
	//private long pos;

	public CustomLineRecordReader(Configuration job, FileSplit split) throws IOException {
		super(job, split);
		// TODO Auto-generated constructor stub
		
	}
	
	public void setPos(long newPos){
		pos = newPos;
	}
	
	
}*/

class CustomInputFormat extends FileInputFormat<Text, Text> {

	//public static int k = 0;
	//public static final int MIN_SPLIT_SIZE = 280;
	public static final int MIN_SPLIT_SIZE = 0;
	public static final int MAX_SPLIT_SIZE = 54488965;
	//public static final int MAX_SPLIT_SIZE = 54488946;54488992
	
	@Override
	protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
	          return super.computeSplitSize(blockSize, MIN_SPLIT_SIZE, MAX_SPLIT_SIZE);
	}
	
	public RecordReader<Text, Text> getRecordReader(InputSplit input, JobConf job, Reporter reporter) throws IOException {

		reporter.setStatus(input.toString());
		return new ObjPosRecordReader(job, (FileSplit)input);
	}


class ObjPosRecordReader implements RecordReader<Text, Text> {

	  private LineRecordReader lineReader;
	  private LongWritable lineKey;
	  private Text lineValue;

	  public ObjPosRecordReader(JobConf job, FileSplit split) throws IOException {
	    lineReader = new LineRecordReader(job, split);

	    lineKey = lineReader.createKey();
	    lineValue = lineReader.createValue();
	  }

	  public boolean next(Text key, Text value) throws IOException {
		
		  //System.out.println(lineReader.getPos());
		 	  
		// get the next line
	    if (!lineReader.next(lineKey, lineValue)) {
	      return false;
	    }
	    
	    //System.out.println(lineKey.toString());
	    
	    boolean wrongStart = false;
	    
	    if(lineValue.toString().contains("{") == false){
	    	//lineKey.set(lineKey.get() - 23);
	    	//lineReader.setPos(54488960);
	    	System.out.println(lineReader.getPos());
	    	System.out.println("Wrongly Started with:"+ lineValue.toString());
	    	wrongStart = true;
	    }
	    	

	    //String custId, name, address, salary, gender;
	    HashMap<String,String> hmValue = new HashMap<String,String>();
	    
	    while(lineValue.toString().contains("{") == false)
	    	if(!lineReader.next(lineKey, lineValue))
	    			return false;
	    
	    if(wrongStart == true){
	    	System.out.println("After while loop: "+lineValue.toString());
	    }
	    	    
	    
	    if(lineValue.toString().contains("{")){
	    	while(lineValue.toString().contains("}") == false){
	    		
	    		String [] pieces = lineValue.toString().split(":");
	    		
	    		hmValue.put(pieces[0].replace("{ ", ""), pieces[1].replace(",", ""));
	    			
	    		lineReader.next(lineKey, lineValue);
	    		/*if(!lineReader.next(lineKey, lineValue))
	    			return false;*/
	    
	    	}
	    	//System.out.println("last Line: "+lineValue.toString());
	    }
	    else
	    	return false;

	    // now that we know we'll succeed, overwrite the output objects
	    //k++;
	    key.set(hmValue.get("Customer ID")); // objName is the output key.
	    //key = new Text(hmValue.get("Customer ID")+"\n");
	    //value = new Text(hmValue.get("Customer ID") + "," + hmValue.get("Name") + "," + hmValue.get("Address") + "," + hmValue.get("Salary") + "," + hmValue.get("Gender")+"\n");  
	    value.set(hmValue.get("Customer ID") + "," + hmValue.get("Name") + "," + hmValue.get("Address") + "," + hmValue.get("Salary") + "," + hmValue.get("Gender"));
	    //hmValue.clear();
	    
	    
	    //if(key.toString().equalsIgnoreCase("1") || key.toString().equalsIgnoreCase("400000"))
	    	//System.out.println(key.toString()+","+value.toString());
	    
	    System.out.println(key.toString());
	    
	    return true;
	  }

	  public Text createKey() {
	    return new Text("");
	  }

	  public Text createValue() {
	    return new Text();
	  }

	  public long getPos() throws IOException {
	    return lineReader.getPos();
	  }

	  public void close() throws IOException {
	    lineReader.close();
	  }
	  
	  public float getProgress() throws IOException {
	    return lineReader.getProgress();
	  }
	}
}


public class CustomCustomer {
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       
			String line = value.toString();
			String[] column = line.split(",");
	        
			String salary = column[3];
			String gender = column[4];
			
			output.collect(new Text(salary), new Text(gender));
	        	 
	       }
	}
	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       
	    	 int sumFemale = 0;
	    	 int sumMale = 0;
	    	 
	    	 while(values.hasNext()){
	    		 if(values.next().toString().equalsIgnoreCase("female")){
	    			 sumFemale = sumFemale + 1;
	    		 }
	    		 else
	    			 sumMale = sumMale + 1;
	    	 }
	    	 
	    		 output.collect(key, new Text("Number of Males: "+sumMale+" ; Number of Females: "+sumFemale));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(CustomCustomer.class);
	     conf.setJobName("CustomCustomer");
	     
	     //conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     //conf.setNumMapTasks(2);
	     //conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(CustomInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	   }

}

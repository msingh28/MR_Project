

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;


public class Problem1 {
	/**
	 * Mapper class to apply projection on the flight data
	 * 
	 */
	

	public static class AverageFlighDelayMapper extends
			Mapper<Object, Text, Text, Text> {
		static HashMap<String, String> flightToDelay;
		  @Override
			protected void setup(Context context) {
			  flightToDelay = new HashMap<String, String>();
			}
		  @Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
					//System.out.println(mapTally.size());
					for (Entry<String, String> entry : flightToDelay.entrySet()) {
						Text key = new Text();
						Text value = new Text();
						key.set(entry.getKey());
						value.set(entry.getValue());
						context.write(key, value);
					}
					flightToDelay.clear();
		  }

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',','"');
		
		/**
		 * Key : Byte offset from where the line is being read
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());		
			
			if (line.length > 0 && isValidEntry(line)) {				
				
				String key = (line[0]+ ":" +line[8]).toLowerCase();			
				
				if(flightToDelay.containsKey(key)) {
					String delayCount = flightToDelay.get(key);
					String data[] = delayCount.split(",");
					int delay = Integer.parseInt(data[0])+ Integer.parseInt(line[14]);;
	        		int count = Integer.parseInt(data[1]) + 1;
	        		String newDelayCount = delay+","+count;
	        		flightToDelay.put(key,newDelayCount);
	        	}
				else{
					String initialDeayCount = Integer.parseInt(line[14])+","+1;
					flightToDelay.put(key,initialDeayCount);
				}			
			
			}
		}
		
		/**
		 * Function determines the validity of the input record
		 * @param data
		 * @return
		 */
		private boolean isValidEntry(String[] record){
			
			if(record == null || record.length == 0){
				return false;
			}		
			
			return true;
		}
		
		/**
		 * Function generates the output record string for Map output
		 * @param record : array of string from input record
		 * @return : String comma separated line
		 */
		private String getMapOutputRecord(String[] record){
			StringBuilder output = new StringBuilder();	
			
			output.append(record[0]).append(",");//Year
			output.append(record[8]).append(",");// UniqueCarrier
			output.append(record[14]);				// ArrDelayMinutes
			
			return output.toString();
		}
	}

	/**
	 * Reduce class to apply equi-join on the flight Mapper output data
	 */
	public static class AverageFlightDelayReducer extends
	Reducer<Text,Text,Text,Text> {
	    private Text result = new Text();

	    public void reduce(Text key, Iterable<Text> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	 int sum = 0;
	    	 int count = 0;
	         for (Text val : values) {
	        	 String value= val.toString();
	        	 String data[] = value.split(",");
	           sum += Integer.parseInt(data[0]);
	           count += Integer.parseInt(data[1]);
	         }
	         double avg = sum/(1.0*count);
	      String average = Double.toString(avg);
	      result.set(average);
	      context.write(key, result);
	    }
	  }
		
		

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: AverageFlighDelay <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
	    job.setJarByClass(Problem1.class);
	    job.setMapperClass(AverageFlighDelayMapper.class);
	    job.setReducerClass(AverageFlightDelayReducer.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    if(job.waitForCompletion(true)){	    	
	    	System.exit(0);
	    }
	    System.exit(1);
	}

}



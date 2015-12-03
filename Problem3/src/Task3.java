import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVParser;


public class Task3 {

	private static String ORIGIN;
	private static String DESTINATION;
	private static String DAY_OF_WEEK;
	private static int count=0 ;
	private static TreeMap<String, Integer> delays = new TreeMap<String, Integer>();

	public static class AverageFlighDelayMapper extends
	Mapper<Object, Text, Text, Text> {

		HashMap<String, Integer> uniqueCarrerDelay;
		
		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(',','"');
		
		public void setup(Context context){
			uniqueCarrerDelay = new HashMap<String, Integer>();
		}

		/**
		 * Key : Byte offset from where the line is being read
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			
			if (line.length > 0 && isValidEntry(line)) {				
				
				// Flight should originate and land at given origin and destination
				if (line[16].toLowerCase().equals(Task3.ORIGIN.toLowerCase()) && 
						(line[17].toLowerCase().equals(Task3.DESTINATION.toLowerCase())) &&
								(line[3].equals(Task3.DAY_OF_WEEK))) {
					if(uniqueCarrerDelay.containsKey(line[8])){
						uniqueCarrerDelay.put(line[8], 
								(uniqueCarrerDelay.get(line[8]) + Integer.parseInt(line[14])));
					}
					else{
						uniqueCarrerDelay.put(line[8], Integer.parseInt(line[14]));
					}
				}
			}
		}
		
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Entry<String, Integer> entry: uniqueCarrerDelay.entrySet()){
				context.write(new Text(entry.getKey()), new Text(Integer.toString(entry.getValue())));
			}
			System.out.println(uniqueCarrerDelay);
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

			// If any of required field is missing, we'll ignore the record
			if(record[3].isEmpty() || record[8].isEmpty() ||
					record[14].isEmpty() || record[16].isEmpty() || 
					record[17].isEmpty() ){
				return false;
			}
			
			return true;
		}
	}
	
	public static class AverageFlightDelayReducer extends
	Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException{
			int delay = 0;
			String temp ;
			for(Text val: values){
				temp = val.toString();
				delay = delay + Integer.parseInt(temp);
			}
			if(delays.containsKey(key.toString())){
				delays.put(key.toString(), delays.get(key) + delay);
			}
			else
				delays.put(key.toString(), delay);
		//	context.write(key, result);
		}
	}
	
	public Task3(Configuration conf, String inputPath, String outputPath,
			String origin, String dest, String day_of_week) throws Exception{
		ORIGIN = origin;
		DESTINATION = dest;
		DAY_OF_WEEK = day_of_week;
		Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
		job.setJarByClass(Task3.class);
		job.setMapperClass(AverageFlighDelayMapper.class);
		job.setReducerClass(AverageFlightDelayReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		if(job.waitForCompletion(true)){
			PrintStream file = new PrintStream("Result" + Integer.toString(Task3.count++));
			for(Entry<String, Integer> ent: Task3.delays.entrySet()){
				file.append(ent.getKey() + " " + ent.getValue());
			}
			file.close();
			delays.clear();
			System.out.println(Task3.delays);
			
		}
		else{
			System.exit(1);
		}
	}


}

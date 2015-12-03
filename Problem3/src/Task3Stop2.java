import java.io.IOException;
import java.io.PrintStream;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVParser;

public class Task3Stop2 {

	private static String ORIGIN;
	private static String DESTINATION;
	private static String DAY_OF_WEEK;
	protected static TreeMap<String, Integer> delays = new TreeMap<String, Integer>();

	public static class Task3Stop2Mapper extends
	Mapper<Object, Text, Text, Text> {

		private CSVParser csvParser = new CSVParser(',','"');

		/**
		 * Key : Byte offset from where the line is being read
		 * value : string representing the entire line of flight data
		 */
		public void map(Object offset, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			Text key = new Text();
			Text val = new Text();
			if (line.length > 0 && isValidEntry(line)) {
				
				
				// Set (FlightDate,IntermediateAirPort) as key
				if(line[16].toLowerCase().equals(Task3Stop2.ORIGIN.toLowerCase())){
					key.set((line[17]).toLowerCase());
					val = new Text(Integer.toString((Integer.parseInt(line[14]) )));
				}else{
					key.set((line[16]).toLowerCase());
					val = new Text(Integer.toString((Integer.parseInt(line[15]))));
				}
				
				context.write(key, val);
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

			// If any of required field is missing, we'll ignore the record
			if(record[3].isEmpty() || record[8].isEmpty() || 
					record[4].isEmpty() || record[6].isEmpty() || 
					record[14].isEmpty() || record[15].isEmpty() ||
					record[16].isEmpty() || record[17].isEmpty() || 
					record[21].isEmpty() || record[23].isEmpty()){
				return false;
			}

			// Whether flight was cancelled or diverted
			if(record[21].equals("1") || record[23].equals("1")){
				return false;
			}	

			// flight should not be originated from ORD and arrived at JFK
			// This will be considered as one legged flight
			if (record[16].toLowerCase().equals(Task3Stop2.ORIGIN.toLowerCase()) && 
					record[17].toLowerCase().equals(Task3Stop2.DESTINATION.toLowerCase())) {
				return false;
			}

			// whether flight was originated from ORIGIN or arrived at DESTINATION
			if(! (record[16].toLowerCase().equals(Task3Stop2.ORIGIN.toLowerCase()) 
					|| (record[17].toLowerCase().equals(Task3Stop2.DESTINATION.toLowerCase()))
					)){
				return false;
			}
			if(!(record[3].equals(Task3Stop2.DAY_OF_WEEK))){
				return false;
			}
			return true;
		}

	}

	public static class Task3Stop2Reducer extends
	Reducer<Text, Text, Text, Text> {
	    
	    /**
	     * Reduce call will be made for every unique key value along with the 
	     * list of related records
	     */
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {	
		
			int delay = 0;
			
			for(Text value : values){
				Integer record = Integer.parseInt(value.toString());
				delay = delay + record;
			}
			delays.put(key.toString(), delay);
		}

		
		/**
	     * cleanup will be called once per Map Task after all the Map function calls,
	     * we'll write hash of words to the file here
	    */
	    protected void cleanup(Context context) throws IOException, InterruptedException{
	 
	    
	    }

	}

	/**
	 * @param args
	 */
	public Task3Stop2(Configuration conf, String inputPath, String outputPath,
			String origin, String dest, String day_of_week) throws Exception {

		ORIGIN = origin;
		DESTINATION = dest;
		DAY_OF_WEEK = day_of_week;
		
		Job job = new Job(conf, "Average flight dealy calculator for 2-legged flights");
		job.setJarByClass(Task3.class);
		job.setMapperClass(Task3Stop2Mapper.class);
		job.setReducerClass(Task3Stop2Reducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		if(job.waitForCompletion(true)){
			PrintStream file = new PrintStream("BestIntermediateAirports");
			for(Entry<String, Integer> ent: Task3Stop2.delays.entrySet()){
				file.append(ent.getKey() + " " + ent.getValue());
			}
			file.close();
			System.out.println(delays);
			System.out.println(DAY_OF_WEEK);
			
		}
		else{
			System.exit(1);
		}
	}


}

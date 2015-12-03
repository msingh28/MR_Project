
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.GenericOptionsParser;


public class BestFlights {
	
	public static String ORIGIN;
	public static String DESTINATION;
	public static int NUMBER_OF_STOPS;
	public static String DAY_OF_WEEK;


	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.err.println("Usage: AverageFlighDelay <in> <out>");
			System.exit(2);
		}
		
		BestFlights.ORIGIN = args[2];
		BestFlights.DESTINATION = args[3];
		BestFlights.DAY_OF_WEEK = args[4];
		BestFlights.NUMBER_OF_STOPS = Integer.parseInt(args[5]);
		
		if(BestFlights.NUMBER_OF_STOPS == 1){
			new Task3(conf, otherArgs[0], otherArgs[1], otherArgs[2], otherArgs[3], otherArgs[4]);
		}
		else
		{
			new Task3Stop2(conf, otherArgs[0], otherArgs[1], otherArgs[2], otherArgs[3], otherArgs[4]);
			File file = new File("BestIntermediateAirports");
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int count = 0;
			String[] words = null;
			while((line = br.readLine())!=null){
				words = line.split(" ");
				System.out.println(words[0]);
				new Task3(conf, otherArgs[0], otherArgs[1] + Integer.toString(count++), words[0], otherArgs[3], otherArgs[4]);
				new Task3(conf, otherArgs[0], otherArgs[1] + Integer.toString(count++), otherArgs[2], words[0], otherArgs[4]);
			}
			fr.close();
			br.close();
		}
		
		
	}

}

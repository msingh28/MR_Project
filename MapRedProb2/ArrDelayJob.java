import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.CSVParser;

public class ArrDelayJob {
	public static class ArrDelayMapper extends Mapper<Object, Text, Text, Text> {
		CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
			String[] line = this.csvParser.parseLine(value.toString());
			if (line.length > 4) {
				StringBuilder key = new StringBuilder();
				// year source destination
				key.append(line[0].trim() + " " + line[16].trim() + " " + line[17].trim());
				// P airlines delay
				// handles spaces in uniquecarrier codes
				if (line[8].trim().split(" ").length > 1) {
					try {
						context.write(new Text(key.toString()), new Text("P " + line[8].trim().split(" ")[0]
								+ line[8].trim().split(" ")[1] + " " + Double.parseDouble(line[14].trim())));
					} catch (NumberFormatException e) {

					}

				} else {
					try {
						context.write(new Text(key.toString()),
								new Text("P " + line[8].trim() + " " + Double.parseDouble(line[14].trim())));
					} catch (NumberFormatException e) {

					}
				}
			} else {
				StringBuilder key = new StringBuilder();
				String[] prevOutputSplit = line[1].split("\\s+");
				// year source destination
				try {
					key.append(line[0].trim() + " " + prevOutputSplit[1].trim() + " " + line[2].trim());
				} catch (ArrayIndexOutOfBoundsException e) {
					for (int i = 0; i < line.length; i++)
						System.out.print(i + line[i].toString());
					System.out.println();
				}
				// Q
				context.write(new Text(key.toString()), new Text("Q "));
			}
		}
	}

	public static class ArrDelayReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> p = new ArrayList<String>();
			List<String> q = new ArrayList<String>();
			Map<String, Double> delayByAirlines = new HashMap<String, Double>();
			for (Text value : values) {
				if (value.toString().contains("P "))
					p.add(value.toString());
				else if (value.toString().contains("Q "))
					q.add(value.toString());
			}
			if (q.size() > 0)
				for (String r : p) {
					String[] pRecord = r.split(" ");
					if (delayByAirlines.containsKey(pRecord[1].trim()))
						delayByAirlines.put(pRecord[1].trim(),
								delayByAirlines.get(pRecord[1].trim()) + Double.parseDouble(pRecord[2].trim()));
					else
						// since some airlines have spaces in their names we
						// cannot split by space in that case
						try {
							delayByAirlines.put(pRecord[1].trim(), Double.parseDouble(pRecord[2].trim()));
						} catch (NumberFormatException e) {
							delayByAirlines.put(pRecord[1].trim() + " " + pRecord[2].trim(),
									Double.parseDouble(pRecord[3].trim()));
						}
				}

			for (Map.Entry<String, Double> e : delayByAirlines.entrySet())
				context.write(new Text(key.toString().split(" ")[0]),
						new Text(e.getKey() + " " + "D" + " " + e.getValue() * -1));
		}
	}

}

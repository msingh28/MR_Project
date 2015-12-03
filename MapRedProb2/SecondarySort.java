import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opencsv.CSVParser;

public class SecondarySort {

	public static class SecondarySortMapper extends Mapper<Object, Text, Key, Text> {
		CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			Key k = new Key();
			k.year = Integer.parseInt(line[0].trim());
			k.count = Double.parseDouble(line[3].trim());
			//year count|negative delay : KEY
			//source,destination : VALUE
			context.write(k, new Text(line[1].trim() + "," + line[2].trim()));
		}
	}

	public static class SecondarySortPartitioner extends Partitioner<Key, Text> {
		public int getPartition(Key key, Text value, int numReducers) {
			return key.year-1987;
		}
	}

	public static class SecondarySortKeyComparator extends WritableComparator {
		protected SecondarySortKeyComparator() {
			super(Key.class, true);
		}

		public int compare(WritableComparable keyA, WritableComparable keyB) {
			Key kA = (Key) keyA;
			Key kB = (Key) keyB;
			int result = kA.year.compareTo(kB.year);
			if (0 == result)
				result = -1 * kA.count.compareTo(kB.count);

			return result;
		}
	}

	public static class SecondarySortGroupingComparator extends WritableComparator {
		protected SecondarySortGroupingComparator() {
			super(Key.class, true);
		}

		public int compare(WritableComparable keyA, WritableComparable keyB) {
			Key kA = (Key) keyA;
			Key kB = (Key) keyB;
			return kA.year.compareTo(kB.year);
		}
	}

	public static class SecondarySortReducer extends Reducer<Key, Text, Text, Text> {

		public void reduce(Key key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int top = 0;
			for (Text v : values)
				if (top < 10) {
					//year,delay airlines
					if (v.toString().split(",")[1].trim().equals("D"))
						context.write(new Text(key.year + "," + key.count * -1), new Text(v.toString().split(",")[0]));
					//year,count source,destination
					else
						context.write(new Text(key.year + "," + key.count), v);
					top++;
				}
		}
	}

}

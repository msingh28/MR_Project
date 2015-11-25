import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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

	public static class SecondarySortMapper extends Mapper<Object, Text, Text, Text> {
		CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\\s+");
			context.write(new Text(line[0].trim() + " " + line[3].trim()),
					new Text(line[1].trim() + " " + line[2].trim()));
		}
	}

	public static class SecondarySortPartitioner extends Partitioner<Text, Text> {
		public int getPartition(Text key, Text value, int numReducers) {
			String[] keyString = key.toString().split(" ");
			Integer year = Integer.parseInt(keyString[0]);
			if (year == 1987)
				return 0;
			else
				return 1;
		}
	}

	public static class SecondarySortKeyComparator extends WritableComparator {
		protected SecondarySortKeyComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparator keyA, WritableComparator keyB) {
			String[] keyAString = keyA.toString().split(" ");
			String[] keyBString = keyB.toString().split(" ");
			Integer yearA = Integer.parseInt(keyAString[0]);
			Integer yearB = Integer.parseInt(keyBString[0]);
			Integer countA = Integer.parseInt(keyAString[1]);
			Integer countB = Integer.parseInt(keyBString[1]);
			int result = yearA.compareTo(yearB);
			if (result == 0)
				result = countB.compareTo(countA);
			return result;
		}
	}

	public static class SecondarySortGroupingComparator extends WritableComparator {
		protected SecondarySortGroupingComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparator keyA, WritableComparator keyB) {
			String[] keyAString = keyA.toString().split(" ");
			String[] keyBString = keyB.toString().split(" ");
			Integer yearA = Integer.parseInt(keyAString[0]);
			Integer yearB = Integer.parseInt(keyBString[0]);
			return -1 * yearA.compareTo(yearB);
		}
	}

	public static class SecondarySortReducer extends Reducer<Text, Text, Text, Text> {
		int top = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values)
				if (top < 3) {
					context.write(key, value);
					top++;
				}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: SecondarySort <inputFile> [<inputFile>...] <outputFile>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Top 3 routes for each year");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setPartitionerClass(SecondarySortPartitioner.class);
		//job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
		job.setSortComparatorClass(SecondarySortKeyComparator.class);
		job.setNumReduceTasks(2);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; i++)
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(new Path("output"))) {
			fs.delete(new Path("output"), true);
		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.opencsv.CSVParser;

public class FrequentRoute {

	public static class FrequentRouteMapper extends Mapper<Object, Text, Text, IntWritable> {
		CSVParser csvParser = new CSVParser(',', '"');

		public void map(Object offset, Text value, Context context) throws IOException, InterruptedException {
			StringBuilder key = new StringBuilder();
			String[] line = this.csvParser.parseLine(value.toString());
			if (line.length > 0) {
				//year source destination
				key.append(line[0] + " " + line[16] + " " + line[17]);
			}
			if(!key.toString().contains("Year"))
			context.write(new Text(key.toString()), new IntWritable(1));
		}
	}

	public static class FrequentRouteReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer sum = 0;
			for (IntWritable value : values)
				sum += Integer.parseInt(value.toString());
			//year source destination count
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// Count every occurrence : Part 1
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: FrequentRoute <inputFile> [<inputFile>...] <outputFile>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Calculating occurrences of pair of source and destination airports by every year");
		job.setJarByClass(FrequentRoute.class);
		job.setMapperClass(FrequentRouteMapper.class);
		job.setReducerClass(FrequentRouteReducer.class);
		//job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/RouteCountsByYear"));
		FileSystem fs = FileSystem.newInstance(conf);

		if (fs.exists(new Path("output/RouteCountsByYear"))) {
			fs.delete(new Path("output/RouteCountsByYear"), true);
		}

		job.waitForCompletion(true);
		// Secondary Sort : Part 2

		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "Top 3 routes for each year");
		job1.setJarByClass(SecondarySort.class);
		job1.setMapperClass(SecondarySort.SecondarySortMapper.class);
		job1.setReducerClass(SecondarySort.SecondarySortReducer.class);
		job1.setPartitionerClass(SecondarySort.SecondarySortPartitioner.class);
		job1.setGroupingComparatorClass(SecondarySort.SecondarySortGroupingComparator.class);
		job1.setSortComparatorClass(SecondarySort.SecondarySortKeyComparator.class);
		job1.setNumReduceTasks(22);
		job1.setMapOutputKeyClass(Key.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(job1, true);
		FileInputFormat.setInputPaths(job1, new Path("output/RouteCountsByYear"));
		FileOutputFormat.setOutputPath(job1, new Path("output/TopBusiest"));
		FileSystem fs1 = FileSystem.newInstance(conf1);

		if (fs1.exists(new Path("output/TopBusiest"))) {
			fs1.delete(new Path("output/TopBusiest"), true);
		}
		job1.waitForCompletion(true);

		/*
		 * Equi Join original data set with top 10 routes obtained from part2 to
		 * calculate the total delay for every route by year and the airlines
		 */
		Configuration conf2 = new Configuration();
		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "Total arrival delay for busiest routes by year and the airlines");
		job2.setJarByClass(ArrDelayJob.class);
		job2.setMapperClass(ArrDelayJob.ArrDelayMapper.class);
		job2.setReducerClass(ArrDelayJob.ArrDelayReducer.class);
		//job2.setNumReduceTasks(2);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(job2, true);

		FileInputFormat.setInputPaths(job2, new Path("output/TopBusiest"));
		FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job2, new Path("output/TotalArrDelayByYearAndByAirlines"));
		FileSystem fs2 = FileSystem.newInstance(conf2);

		if (fs2.exists(new Path("output/TotalArrDelayByYearAndByAirlines"))) {
			fs2.delete(new Path("output/TotalArrDelayByYearAndByAirlines"), true);
		}
		job2.waitForCompletion(true);

		// The best performing airlines in the busiest routes : Secondary Sort : Part 4

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf1, "The best performing airlines in the busiest routes");
		job3.setJarByClass(SecondarySort.class);
		job3.setMapperClass(SecondarySort.SecondarySortMapper.class);
		job3.setReducerClass(SecondarySort.SecondarySortReducer.class);
		job3.setPartitionerClass(SecondarySort.SecondarySortPartitioner.class);
		job3.setGroupingComparatorClass(SecondarySort.SecondarySortGroupingComparator.class);
		job3.setSortComparatorClass(SecondarySort.SecondarySortKeyComparator.class);
		job3.setNumReduceTasks(22);
		job3.setMapOutputKeyClass(Key.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.setInputDirRecursive(job3, true);
		FileInputFormat.setInputPaths(job3, new Path("output/TotalArrDelayByYearAndByAirlines"));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[otherArgs.length - 1]));
		FileSystem fs3 = FileSystem.newInstance(conf3);

		if (fs3.exists(new Path(otherArgs[otherArgs.length - 1]))) {
			fs3.delete(new Path(otherArgs[otherArgs.length - 1]), true);
		}
		System.exit(job3.waitForCompletion(true) ? 0 : 1);

	}

}

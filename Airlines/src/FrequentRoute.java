import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
				key.append(line[0] +" "+ line[16] +" "+ line[17]);
			}
			context.write(new Text(key.toString()), new IntWritable(1));
		}
	}

	public static class FrequentRouteReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Integer sum = 0;
			for (IntWritable value : values)
				sum += Integer.parseInt(value.toString());
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: FrequentRoute <inputFile> [<inputFile>...] <outputFile>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Busiest Route");
		job.setJarByClass(FrequentRoute.class);
		job.setMapperClass(FrequentRouteMapper.class);
		job.setReducerClass(FrequentRouteReducer.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

package weatherData;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WeatherDataExtraction {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException {
			String data = line.toString();
			StringTokenizer tokenizer = new StringTokenizer(data, " \tC");
			ArrayList<String> arrayList = new ArrayList<>(tokenizer.countTokens());
			while (tokenizer.hasMoreTokens()) {
				arrayList.add(tokenizer.nextToken());
			}
			for (int i = 2; i < arrayList.size(); ++i) {
				context.write(new IntWritable(i), new DoubleWritable(Double.parseDouble(arrayList.get(i))));
			}
		}
	}
	public static class Reduce extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			for (DoubleWritable x : values) {
				sum += x.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));	
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WeatherDataExtraction");
		job.setJarByClass(WeatherDataExtraction.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

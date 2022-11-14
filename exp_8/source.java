package bloomFilter;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class BloomFilter {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final int MOD = 97; 
			int number = Integer.parseInt(value.toString());
			int firstResult = number % MOD;
			int secondResult = (2 * number + 7) % MOD;
			context.write(new IntWritable(-1), new IntWritable(firstResult));
			context.write(new IntWritable(-1), new IntWritable(secondResult));
		}
	}
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			SortedSet<Integer> sortedSet = new TreeSet<>();
			for (IntWritable x : values) {
				sortedSet.add(x.get());
			}
			for (int x : sortedSet) {				
				context.write(new IntWritable(x), new IntWritable(1));
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BloomFilter");
		job.setJarByClass(BloomFilter.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

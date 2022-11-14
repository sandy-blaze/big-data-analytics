package semiJoin;

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

public class SemiJoin {
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public void map(LongWritable index, Text value, Context context) throws IOException, InterruptedException {
			String[] vals = value.toString().split(" ");
			int table = Integer.parseInt(vals[0]);
			int rowValue = Integer.parseInt(vals[1]);
			context.write(new IntWritable(table), new IntWritable(rowValue));
		}
	}
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		SortedSet<Integer> firstTable, secondTable;
		public Reduce() {
			firstTable = new TreeSet<>();
			secondTable = new TreeSet<>();
		}
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int tab = Integer.parseInt(key.toString());
			for (IntWritable x : values) {
				if (tab == 1) {
					firstTable.add(x.get());
				} else {
					secondTable.add(x.get());
				}
			}
			if (firstTable.size() == 0 || secondTable.size() == 0) {
				return;
			}
			int index = 1;
			for (int x : firstTable) {
				if (secondTable.contains(x)) {
					context.write(new IntWritable(index++), new IntWritable(x));
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SemiJoin");
		job.setJarByClass(SemiJoin.class);
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

package testHadoop;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class TestHadoop {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException {
			String data = line.toString();
			StringTokenizer tokenizer = new StringTokenizer(data, " \tC");
			ArrayList<String> arrayList = new ArrayList<>(tokenizer.countTokens());
			while (tokenizer.hasMoreTokens()) {
				arrayList.add(tokenizer.nextToken());
			}
			for (int i = 2; i < arrayList.size(); ++i) {
				context.write(new Text(String.format("%04d", i)), new Text(arrayList.get(i) + ",1"));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			for (Text x : values) {
				String[] s = x.toString().split(",");
				sum += Double.parseDouble(s[0]);
				count += Integer.parseInt(s[1]);
			}
			context.write(new Text("Index: " + key.toString() + ", "),
					new Text(String.format("Average: %12.6f,\tcount: %4d", sum / count, count)));	
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TestHadoop");
		job.setJarByClass(TestHadoop.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

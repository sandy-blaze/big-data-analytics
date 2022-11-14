package pageRanking;

import java.io.IOException;
import java.util.*;
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

public class PageRanking {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable index, Text line, Context context) throws IOException, InterruptedException {
			String[] data = line.toString().split(":");
			if (data.length == 1) {
				context.write(new Text("#"), line);
				return;
			}
			Text node = new Text(data[0]);
			for (String x : data[1].split(",")) {
				context.write(node, new Text(x));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private final double nearZero = 0.0001;
		private int graphSize = -1, size = 0;
		private ArrayList<Integer>[] outGraph, inGraph;
		private double[] scores;
		private int Int(Text value) {
			return Integer.parseInt(value.toString());
		}
		private void setRanks() {
			double[] newScores = new double[graphSize];
			while (true) {
				for (int i = 0; i < graphSize; ++i) {					
					newScores[i] = 0;
					for (int x : inGraph[i]) {
						newScores[i] += scores[x] / outGraph[x].size();
					}
				}
				boolean isStable = true;
				for (int i = 0; i < graphSize; ++i) {
					if (Math.abs(scores[i] - newScores[i]) > nearZero)
						isStable = false;
					scores[i] = newScores[i];
				}
				if (isStable)
					return;
			}
		}
		@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.toString().charAt(0) == '#') {
				for (Text x : values) {					
					graphSize = Int(x);
				}
				outGraph = new ArrayList[graphSize];
				inGraph = new ArrayList[graphSize];
				scores = new double[graphSize];
				for (int i = 0; i < graphSize; ++i) {
					outGraph[i] = new ArrayList<>();
					inGraph[i] = new ArrayList<>();
					scores[i] = 1.0 / graphSize;
				}
				return;
			}
			int u = Int(key);
			for (Text x : values) {
				outGraph[u].add(Int(x));
				inGraph[Int(x)].add(u);
			}
			if (++size < graphSize) return;
			setRanks();
			SortedMap<Double, Integer> sortedMap = new TreeMap<>();
			for (int i = 0; i < graphSize; ++i) {
				sortedMap.put(-scores[i], i);
			}
			int rank = 0;
			for (Integer v : sortedMap.values()) {
				context.write(new Text(v + ""), new Text("Rank : " + ++rank));
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRanking");
		job.setJarByClass(PageRanking.class);
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

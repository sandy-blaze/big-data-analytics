#sandy
package shortestPath;

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

public class ShortestPath {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
			String[] data = line.toString().split(":");
			if (data.length == 1) {
				context.write(new Text("#"), line);
				return;
			}
			Text node = new Text(data[0]);
			for (String friend : data[1].split(",")) {
				context.write(node, new Text(friend));
			}
		}
	}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private int start, maxSize, end, size = 0;
		private ArrayList<Integer>[] graph;
		private int Int(Text value) {
			return Integer.parseInt(value.toString());
		}
		private int Int(String value) {
			return Integer.parseInt(value);
		}
		private String getDistance() {
			int n = maxSize;
			int[] parent = new int[n];
			boolean[] visited = new boolean[n];
			for (int i = 0; i < n; ++i) {
				parent[i] = -1;
			}
			Queue<Integer> queue = new LinkedList<>();
			queue.add(start);
			while (queue.size() > 0) {
				int node = queue.remove();
				visited[node] = true;
				if (node == end) {
					LinkedList<String> pathNodes = new LinkedList<>();
					while (parent[node] != -1) {						
						pathNodes.addFirst(node + "");
						node = parent[node];
					}
					pathNodes.addFirst(start + "");
					return String.join(" -> ", pathNodes);
				}
				for (int x : graph[node]) {
					if (!visited[x]) {
						parent[x] = node;
						queue.add(x);
					}
				}
			}
			return "No Path";
		}
		@SuppressWarnings("unchecked")
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.toString().charAt(0) == '#') {
				String[] cords = {"0", "0", "0"};
				for (Text val : values) {
					cords = val.toString().split(",");
				}
				maxSize = Int(cords[0]);
				start = Int(cords[1]);
				end = Int(cords[2]);
				graph = new ArrayList[maxSize];
				return;
			}
			int u = Int(key);
			graph[u] = new ArrayList<>();
			for (Text x : values) {
				graph[u].add(Int(x));
			}
			if (++size < maxSize)
				return;
			context.write(new Text("Path : "), new Text(getDistance()));
		}
		public Reduce() {
			start = end = maxSize = -1;
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ShortestPath");
		job.setJarByClass(ShortestPath.class);
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

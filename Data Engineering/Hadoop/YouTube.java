import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class Y {
	public String category;
	public double rating;
	
	public Y(String _category, double _rating){
		this.category = _category;
		this.rating = _rating;
	}
	
	public String getString() {
		String shortR = String.format("%.4f", rating);
		String rslt = category + " " + shortR;
		return rslt;
	}
}

public class YouTubeStudent {
	
	public static class YComparator implements Comparator<Y> 
	{
		public int compare(Y x, Y y) {
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertY(PriorityQueue q, String category, double rating, int topK) 
	{
		Y y_head = (Y)q.peek();
		if (q.size() < topK || y_head.rating < rating)
		{
			Y y = new Y(category, rating);
			q.add(y);
			if (q.size() > topK) q.remove();
		}
	}
	
	public static class YouTubeStudentMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private Text category = new Text();
		private DoubleWritable rating = new DoubleWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String split[] = value.toString().split("[|]");
			double rt = Double.parseDouble(split[6]);
			rating.set(rt);
			
			StringTokenizer itr = new StringTokenizer(split[3], "&");
			
			while (itr.hasMoreTokens())
			{
				String ct = itr.nextToken().trim();
				category.set(ct);
				context.write(category, rating);
			}
		}
	}
	
	public static class YouTubeStudentReducer extends Reducer<Text, DoubleWritable, Text, NullWritable>
	{
		private PriorityQueue<Y> queue;
		private Comparator<Y> comp = new YComparator();
		private int topK;
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0;
			int idx = 0;
			double avg = 0;
			for (DoubleWritable val : values)
			{
				sum += val.get();
				idx++;
			}
			avg = sum / (double) idx;
			
			String category = key.toString();
			insertY(queue, category, avg, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Y>(topK, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Y y = (Y) queue.remove();
				context.write(new Text(y.getString()), NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = 2;
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopK <in> <out> <topK>");
			System.exit(2);
		}
		conf.setInt("topK", topK);
		Job job = new Job(conf, "youtube");
		job.setJarByClass(YouTubeStudent.class);
		job.setMapperClass(YouTubeStudentMapper.class);
		job.setReducerClass(YouTubeStudentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		
		job.waitForCompletion(true);
	}
}

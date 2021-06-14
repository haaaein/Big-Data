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

public class Kmeans {

	public class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
	    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    	Text result = new Text();
			StringBuffer values_buffer = new StringBuffer();
			for (Text val : values) 
			{
				values_buffer.append( val.toString() );
				values_buffer.append(" ");
			}
			result.set(values_buffer.toString());
			context.write(key, result);
	    }
	}
	
	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		private IntWritable one_key = new IntWritable();
		
		private int n_centers;
		private double[] center_x;
		private double[] center_y;
		
		public double getDist(double x1, double y1, double x2, double y2) {
			double dist = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
			return Math.sqrt( dist );
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if (itr.countTokens() < 2) return;
			if (n_centers == 0) return;
			
			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			int cluster_idx = 0;
			double min_dist = Double.MAX_VALUE;
			
			for (int i = 0; i < n_centers; i++) 
			{
				double dist = getDist(x, y, center_x[i], center_y[i]);
				if (min_dist > dist) 
				{
					cluster_idx = i; //중심점2개 잡았으니까.	
					min_dist = dist;
				}
			}
			one_key.set(cluster_idx);
			context.write(one_key, value);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			n_centers = conf.getInt("n_centers", -1);
			center_x = new double[n_centers];
			center_y = new double[n_centers];
			
			for (int i = 0; i < n_centers; i++) {
				center_x[i] = conf.getFloat("center_x_" + i, 0);
				center_y[i] = conf.getFloat("center_y_" + i, 0);
			}
		}
	}
	
	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			Text result = new Text();
			
			StringTokenizer s = new StringTokenizer(value.toString().trim());
			while (s.hasMoreTokens()) {
				StringTokenizer itr = new StringTokenizer(s.toString().trim());
				x_total += Double.parseDouble(itr.nextToken().trim());
				y_total += Double.parseDouble(itr.nextToken().trim());
				cnt++;
			}
			
			x_total /= cnt;
			y_total /= cnt;
			String result_string = x_total + " " + y_total;
			result.set(result_string);
			context.write(key, result);
		}
	}
	
	public static void updateKMeans( Configuration conf, int n_pages) throws Exception {
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path("output/part-r-00000");
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader( new InputStreamReader(in));
		
		String line = reader.readLine();
		while( line != null)
		{
			StringTokenizer itr = new StringTokenizer(new String( line ));
			int cl_id = Integer.parseInt(itr.nextToken().trim());
			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			conf.setFloat("center_x_" + cl_id, (float)x);
			conf.setFloat("center_y_" + cl_id, (float)y);
			System.out.println( cl_id + " " + x + " " + y );
			line = reader.readLine();
		}
	}
	
	public static void initKMeans(Configuration conf, int n_centers)
	{
		conf.setInt("n_centers", n_centers);
		Random r = new Random();
		for (int i = 0; i < n_centers; i++) {
			conf.setFloat( "center_x_" + i, r.nextInt(6));
			conf.setFloat("center_y_" + i, r.nextInt(6));
		}
	}
	
	public static void main(String[] srgs) throws Exception 
	{
		int n_centers = 2;
		int n_iter = 3;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: KMeans");
			System.exit(2);
		}
		
		for (int i = 0; i < n_iter; i++)
		{
			Job job = new Job(conf, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
			
			job.waitForCompletion(true);
			updateKMeans(conf, n_centers);
		}
	}
}

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

public class IMDBStudent {

	public static class IMDBStudentMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text genre = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String split[] = value.toString().split("::");
			
			StringTokenizer itr = new StringTokenizer(split[2], "|");
			while (itr.hasMoreTokens())
			{
				genre.set(itr.nextToken());
				context.write(genre, one);
			}
			
		}
	}
	
	public static class IMDBStudentReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable result = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for (LongWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "genrecount");

		job.setJarByClass(IMDBStudent.class);
		job.setMapperClass(IMDBStudentMapper.class);
		job.setCombinerClass(IMDBStudentReducer.class);
		job.setReducerClass(IMDBStudentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		
		job.waitForCompletion(true);
	}
}
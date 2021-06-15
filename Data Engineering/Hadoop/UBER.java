import java.io.IOException;
import java.util.*;

import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text key_result = new Text();
		private Text value_result = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String split[] = value.toString().split(",");
			String region = "", day = "", trips = "", vehicles = "";
			
			region = split[0];
			day = split[1];
			trips = split[3];
			vehicles = split[2];
			
			SimpleDateFormat format1=new SimpleDateFormat("MM/dd/yyyy");
			Date dt1 = null;
			try {
				dt1 = format1.parse(day);
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
		
			DateFormat format2=new SimpleDateFormat("EE"); 
			String finalDay=format2.format(dt1);
			String Day = finalDay.toUpperCase();
			if (Day.equals("THU"))
				Day = "THR";
			key_result.set(region + "," + Day);
			value_result.set(trips + "," + vehicles);
			
			context.write(key_result, value_result);
		}
		
	}
	
	public static class UBERReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			
			int vehicles = 0, trips = 0;
			int totalV = 0, totalT = 0;
			String value_result = "";
			
			for (Text val : values) {
				String s[] = val.toString().split(",");
				
				vehicles = Integer.parseInt(s[1]);
				trips = Integer.parseInt(s[0]);
				
				totalV += vehicles;
				totalT += trips;
			}
			
			value_result = totalT + "," + totalV;
			result.set(value_result);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "uber");
		
		job.setJarByClass(UBERStudent.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}

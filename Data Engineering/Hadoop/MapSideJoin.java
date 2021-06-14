import java.io.BufferedReader;
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

import org.apache.hadoop.filecache.DistributedCache;
import java.util.*;

public class MapSideJoin {

	Hashtable<String, String> joinMap = new Hashtable<String, String>();
	
	public static class MapSideJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		StringTokenizer itr = new StringTokenizer(value.toString(), "|");
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		String ID = itr.nextToken();
		String price = itr.nextToken();
		String category = itr.nextToken();
		String o_value = price + "\t" + joinMap.get( category );
		
		outputKey.set( ID );
		outputValue.set( o_value );
		context.write( outputKey, outputValue );
	}
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles( context.getConfiguration());
		BufferedReader br = new BufferedReader( new FileReader( cacheFiles[0].toString() ));
		String line = br.readLine();
		
		while (line != null)
		{
			StringTokenizer itr = new StringTokenizer(line, "|");
			String category = itr.nextToken();
			String category_name = itr.nextToken();
			joinMap.put( category, category_name);
			line = br.readLine();
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int topK = 3;
		if (otherArgs.length != 2)
		{
			System.err.println("Usage");
			System.exit(2);
		}
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "MapSideJoin");
		DistributedCache.addCacheFile( new URI("/MapSideJoin"), job.getConfiguration() );
		job.setJarByClass(MapSideJoin.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.serNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		
		job.waitForCompletion(true);
	}
}

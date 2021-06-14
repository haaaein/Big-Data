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

public class PageRank {

	public static class PageRankMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		
		private IntWritable one_key = new IntWritable();
		private DoubleWritable one_value = new DoubleWritable();
		
		private int n_pages;
		private double[] pagerank;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			//0 1 2 3 이면 첫번째는 소스, 두번째부터가 타겟 페이지.
			StringTokenizer itr = new StringTokenizer(value.toString());
			int n_links = itr.countTokens() - 1; //그 페이지가 링크 걸고 있는 개수.
			if (n_links == 0) return;//링크 안걸고있으면 return.
			
			int src_id = Integer.parseInt(itr.nextToken().trim()); //소스 페이지. 첫번째 값.
			int target_id;
			double pr = pagerank[src_id] / (double) n_links; //현재 page rank 값 / 링크 
			one_value.set( pr );
			//연결된 page에 page rank 값들을 분배
			
			while (itr.hasMoreTokens()) {
				target_id = Integer.parseInt( itr.nextToken().trim() );
				one_key.set( target_id );
				context.write( one_key, one_value );
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1); //웹페이지 개수 얻어오기
			pagerank = new double[n_pages];
			for (int i = 0; i < n_pages; i++) 
			{
				pagerank[i] = conf.getFloat( "pagerank" + i, 0 ); //현재 페이지 랭크 값
			}
		}
	}
	
	public static class PageRankReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
	{
		private DoubleWritable result = new DoubleWritable();
		private double damping_factor = 0.85;
		private int n_pages;
		
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double agg_val = 0;
			agg_val = (1.0 - damping_factor) / (double)n_pages; //이탈확률 0.15 / 4(총 페이지 개수)	
			for (DoubleWritable val : values) {
				agg_val = agg_val + damping_factor * val.get();
			}
			result.set( agg_val );
			context.write(key, result);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			n_pages = conf.getInt("n_pages", -1); //웹페이지 개수 얻어오기
		}
	}
	
	public static void initPageRank(Configuration conf, int n_pages)
	{ 
		conf.setInt("n_pages", n_pages); //웹페이지 개수 전달
		for(int i = 0; i < n_pages; i++)
			conf.setFloat( "pagerank"+i, (float)(1.0/(double)n_pages)); //최초의 page rank setup
	}
	
	public static void updatePageRank( Configuration conf, int n_pages) throws Exception
	{
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path( "/user/yeohaein/output/part-r-00000" );
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader( new InputStreamReader(in) );
		
		String line = reader.readLine(); 
		while( line != null )
		{
			StringTokenizer itr = new StringTokenizer(new String( line ) ) ; 
			int src_id = Integer.parseInt(itr.nextToken().trim());
			double pr = Double.parseDouble(itr.nextToken().trim()); 
			conf.setFloat( "pagerank" + src_id, (float) pr );
			//새롭게 계산된 page rank를 읽어서 저장
			System.out.println( src_id + " " + pr );
			line = reader.readLine(); 
		}
	}
	public static void main(String[] args) throws Exception
	{
		int n_pages = 4; //웹페이지 개수
		int n_iter = 3; //loop 몇 번 돌건지.	
		Configuration conf = new Configuration();
		
		/*
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: PageRank <in> ");
			System.exit(2);
		}
		*/
		
		initPageRank(conf, n_pages);
		
		for (int i = 0; i < n_iter; i++)
		{
			Job job = new Job(conf, "page rank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true));
			job.waitForCompletion(true);
			updatePageRank(conf, n_pages);
		}
	}
}

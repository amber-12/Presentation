

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount22 {

	
	public static class TokenizerMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public final static IntWritable one=new IntWritable(1);
		
		public Text word=new Text();
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr=new StringTokenizer(value.toString(),",");
			while(itr.hasMoreTokens())
			{
				String myword=itr.nextToken().toLowerCase();
				word.set(myword);
				context.write(word,one);
			}
		
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result=new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable val:values)
			{
				sum+=val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf=new Configuration ();
		Job job=new Job(conf);
		job.setJarByClass(WordCount22.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

		
	}

}

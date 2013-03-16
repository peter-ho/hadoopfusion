package fusion.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fusion.hadoop.fusionkeycreation.FusionKeyCreation;


// https://ccp.cloudera.com/display/CDH4DOC/Using+the+CDH4+Maven+Repository
public class WordCountFused
{
	public static class WordCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class WordCountReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main1( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}

		JobConf conf = new JobConf();
		conf.setJarByClass(WordCount.class);
		conf.setJobName("Word Count with Fusion");

		conf.set(FileInputFormat.INPUT_DIR, args[0]);
		conf.set(FileOutputFormat.OUTDIR, args[1]);
		conf.setClass(MRJobConfig.MAP_CLASS_ATTR, WordCountMapper.class, Mapper.class);
		conf.setClass(MRJobConfig.REDUCE_CLASS_ATTR, WordCountReducer.class, Reducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FusionJob job = FusionJob.getInstance(conf);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		
		int status = FusionKeyCreation.main(args[0], "/user/peter/fusion/FusionKeyCreation");
		System.exit(status);
	}

}

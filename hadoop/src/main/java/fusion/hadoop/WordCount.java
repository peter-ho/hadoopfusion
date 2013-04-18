package fusion.hadoop;

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

import fusion.hadoop.fusionexecution.FusionExecution;


// https://ccp.cloudera.com/display/CDH4DOC/Using+the+CDH4+Maven+Repository
public class WordCount 
{
	public static class WordCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			for (String token : WordCountFused.WordCountMapper.map(value)) {
				word.set(token);
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
			FusionExecution.FusionExecutionReducer.compute(key, values);
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
	{
		System.out.println("\n*** WordCount start...");
		long msStart = System.currentTimeMillis();
		if (args.length != 2) 
		{
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", FusionConfiguration.NUM_OF_REDUCERS);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("Word Count");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(FusionConfiguration.NUM_OF_REDUCERS);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean result = job.waitForCompletion(true);
		long msEnd = System.currentTimeMillis();
		System.out.println("\n*** Total elapsed: " + (msEnd - msStart) + "ms");
		System.exit(result ? 0 : 1);
	}
}

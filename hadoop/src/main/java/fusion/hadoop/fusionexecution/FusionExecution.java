package fusion.hadoop.fusionexecution;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class FusionExecution {
	public static class FusionExecutionMapper
	extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class FusionExecutionReducer
	extends Reducer<Text, NullWritable, Text, Text> {

		private Text last = new Text();
		private MultipleOutputs<Text, Text> multipleOutputs;
		private boolean lastConsumed = true;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context)
						throws IOException, InterruptedException {
			if (lastConsumed) {
				last.set(key);
				lastConsumed = false;
			} else {
				lastConsumed = true;
				//context.write(key, new Text(last));
				multipleOutputs.write(key, new Text(last), "fusionkey");
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if (!lastConsumed) multipleOutputs.write(new Text(last), new Text(""), "remainder");			
			multipleOutputs.close();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		main(args[0], args[1], args[2]);
	}

	protected static int executeFusionExecutionJob(String inputPath, String outputPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println("FusionExecutionCreation job begins");
		Job job = Job.getInstance();
		job.setJarByClass(FusionExecution.class);
		job.setJobName("FusionExecution");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(FusionExecutionMapper.class);
		job.setReducerClass(FusionExecutionReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		

		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("FusionExecution job ends with status " + status);
		return status;
	}
	
	public static int main(String inputPath, String fusionKeyPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{		
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);
		
		int status = executeFusionExecutionJob(inputPath, outputPath, fs);
		
		if (status == 0) {
			
		}
		return status;
	}

}

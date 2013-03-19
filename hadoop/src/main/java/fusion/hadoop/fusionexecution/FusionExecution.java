package fusion.hadoop.fusionexecution;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fusion.hadoop.fusionkeycreation.FusionKeyMapParser;


public class FusionExecution {
	public static class FusionExecutionMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		protected FusionKeyMapParser km = new FusionKeyMapParser();		
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = new Configuration();
			Path[] paths = context.getLocalCacheFiles();
			km.initialize(paths, conf);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO: invoke input map class map method
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class FusionExecutionReducer
	extends Reducer<Text, IntWritable, Text, Text> {

		private Text last = new Text();
		private MultipleOutputs<Text, Text> multipleOutputs;
		private boolean lastConsumed = true;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
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
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		main(args[0], args[1], args[2]);
	}

	protected static int executeFusionExecutionJob(String inputPath, String outputPath, String fusionKeyPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		System.out.println("FusionExecutionCreation job begins");
		Configuration conf = new Configuration();
		//addFusionKeyCacheFiles(job, fs, fusionKeyPath);
		addFusionKeyCacheFiles(conf, fs, fusionKeyPath);
		Job job = Job.getInstance(conf);
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

	protected static void addFusionKeyCacheFiles(Configuration job, FileSystem fs, String fusionKeyPath) throws IOException, URISyntaxException {
		addCacheFiles(job, fs, fusionKeyPath + "/remainder-r-*");
		addCacheFiles(job, fs, fusionKeyPath + "/fusionkey-r-*");
	}

	protected static void addCacheFiles(Configuration conf, FileSystem fs, String pattern) throws IOException, URISyntaxException {
		FileStatus[] fss = fs.globStatus(new Path(pattern));
		//ArrayList<URI> uris = new ArrayList<URI>();
		for (FileStatus fst : fss) {
			//job.addCacheFile(new URI(fst.getPath().toString()));
			//uris.add(fst.getPath().toUri());
			DistributedCache.addCacheFile(fst.getPath().toUri(), conf);
			System.out.println("\tadding cache path: " + fst.getPath().toString());
		}
	}
	
	public static int main(String inputPath, String fusionKeyPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);
		
		int status = executeFusionExecutionJob(inputPath, outputPath, fusionKeyPath, fs);
		
		if (status == 0) {
			
		}
		return status;
	}

}

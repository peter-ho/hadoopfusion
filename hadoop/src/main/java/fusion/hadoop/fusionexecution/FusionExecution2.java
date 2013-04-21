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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fusion.hadoop.FusionConfiguration;
import fusion.hadoop.TextPair;
import fusion.hadoop.WordCountFused;
import fusion.hadoop.fusionkeycreation.FusionKeyCreation2;
import fusion.hadoop.fusionkeycreation.FusionKeyMap;
import fusion.hadoop.fusionkeycreation.FusionKeyMapParser;
import fusion.hadoop.fusionkeycreation.MapFileParser;


public class FusionExecution2 {
	public static class FusionExecutionMapper0
		extends Mapper<Text, FusionKeyCreation2.KeyCreationWritable, TextPair, Writable> {
		protected TextPair keyPairRaw = new TextPair();
		protected Text empty = new Text("");
		
		@Override
		public void map(Text key, FusionKeyCreation2.KeyCreationWritable value, Context context)
				throws IOException, InterruptedException {
			
			keyPairRaw.set(key, empty);
			Writable[] values =((FusionKeyCreation2.ValueArrayWritable) value.get()).get();
			for (int i=0; i<values.length; ++i) {
				System.out.println(" mapping: " + key.toString() + " :: " + values[i]);
				context.write(keyPairRaw, values[i]);
			}
		}
	}

	public static class FusionExecutionMapper1
	extends Mapper<Text, FusionKeyCreation2.KeyCreationWritable, TextPair, Writable> {
		protected TextPair keyPairRaw = new TextPair();
		protected Text empty = new Text("");

		@Override
		public void map(Text key, FusionKeyCreation2.KeyCreationWritable value, Context context)
				throws IOException, InterruptedException {

			keyPairRaw.set(empty, key);
			Writable[] values =((FusionKeyCreation2.ValueArrayWritable) value.get()).get();
			for (int i=0; i<values.length; ++i) {
				System.out.println(" mapping: " + key.toString() + " :: " + values[i]);
				context.write(keyPairRaw, values[i]);
			}
		}
	}

	
	public static class FusionExecutionReducer
	extends Reducer<TextPair, IntWritable, Text, IntWritable> {

		private Text fusedKey = new Text();
		private MultipleOutputs<Text, IntWritable> multipleOutputs;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		@Override
		public void reduce(TextPair key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
//			String keyString = key.toString();
//			Text actualKey = new Text(keyString.substring(1));
			if (key.getSecond().toString().length() == 0) {
				/// write to raw key result
				IntWritable value = inputReduce(key.getFirst(), values);
				if (value != null) multipleOutputs.write(key.getFirst(), value, "result");
			} else if (key.getFirst().toString().length() == 0) {
				IntWritable value = inputReduce(key.getSecond(), values);
				if (value != null) multipleOutputs.write(key.getSecond(), value, "result");
			} else {
				fusedKey.set(key.toString());
				IntWritable value = inputReduce(fusedKey, values);
				if (value != null) {
					if (key.getFirst().toString().compareTo(key.getSecond().toString()) != 0) {
						multipleOutputs.write(fusedKey, value, "fused_result");
					} else {
						/// single key with no other key
						multipleOutputs.write(key.getSecond(), value, "fused_result");
					}
				}
			}
		}
		
		protected IntWritable inputReduce(Text key, Iterable<IntWritable> values) {
			try {
				int sum = 0;
				//if (key.toString().compareTo("hadoop") == 0) throw new Exception("Fail to reduce.");
				for (IntWritable value : values) {
					sum += value.get();
				}
				compute(key, values);
				return new IntWritable(sum);
			} catch (Exception ex) {
				System.err.println("Error when reducing key: " + key.toString() + "\n\t" + ex.getMessage());
			}
			return null;
		}
		
		public static void compute(Text key, Iterable<IntWritable> values) {
			///simulate long running process
			for (int k=0; k<0; ++k) {
				for (int i=40001; i<50000; ++i) {
					boolean isPrime = true;
					for (int j=2; j<i; ++j) {
						if ((i % j) == 0) {
							isPrime = false;
							break;
						}
					}
					if (isPrime) { }
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
	
	public static class Partitioner extends FusionPartitioner {
		public Partitioner() {
			super(FusionConfiguration.NUM_OF_REDUCERS);
		}
	}
	
//	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
//		main(args[0], args[1], args[2]);
//	}

//	protected static String FusionKeyPath;
	protected static int executeFusionExecutionJob(String inputPath0, String inputPath1, String outputPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		System.out.println("FusionExecution2 job begins");
//		FusionKeyPath = fusionKeyPath;
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.reduce.maxattempts", 1);
		conf.setInt("mapred.reduce.max.attempts", 1);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.setInt("mapred.max.reduce.failures.percent", 49);
		conf.setInt("mapreduce.job.reduces", FusionConfiguration.NUM_OF_REDUCERS);
		//addFusionKeyCacheFiles(conf, fs, fusionKeyPath);
		Job job = Job.getInstance(conf);
		job.setJarByClass(FusionExecution.class);
		job.setJobName("FusionExecution");

		//FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		MultipleInputs.addInputPath(job, new Path(inputPath0), SequenceFileInputFormat.class, FusionExecutionMapper0.class);
		MultipleInputs.addInputPath(job, new Path(inputPath1), SequenceFileInputFormat.class, FusionExecutionMapper1.class);

		//job.setMapperClass(FusionExecutionMapper.class);
		job.setReducerClass(FusionExecutionReducer.class);
		job.setNumReduceTasks(FusionConfiguration.NUM_OF_REDUCERS);
		job.setCombinerClass(FusionCombiner.class);
		job.setPartitionerClass(Partitioner.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setMaxReduceAttempts(1);	/// reduce retry execution to 1, relying on fusion for fault tolerance
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("FusionExecution2 job ends with status " + status);
		return status;
	}

//	protected static void addFusionKeyCacheFiles(Configuration job, FileSystem fs, String fusionKeyPath) throws IOException, URISyntaxException {
//		addCacheFiles(job, fs, fusionKeyPath + "/fusionkey-r-*");
//	}
//
//	protected static void addCacheFiles(Configuration conf, FileSystem fs, String pattern) throws IOException, URISyntaxException {
//		FileStatus[] fss = fs.globStatus(new Path(pattern));
//		//ArrayList<URI> uris = new ArrayList<URI>();
//		for (FileStatus fst : fss) {
//			//job.addCacheFile(new URI(fst.getPath().toString()));
//			//uris.add(fst.getPath().toUri());
//			DistributedCache.addCacheFile(fst.getPath().toUri(), conf);
//			System.out.println("\tadding cache path: " + fst.getPath().toString());
//		}
//	}
	
//	public static int main(String inputPath0, String inputPath1, String fusionKeyPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	public static int main(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(inputPath + "/part-r-00000"), true);
		fs.delete(new Path(outputPath), true);
		
		int status = executeFusionExecutionJob(inputPath + "/key0*", inputPath + "/key1*", outputPath, fs);
		
		if (status == 0) {
			
		}
		return status;
	}

}

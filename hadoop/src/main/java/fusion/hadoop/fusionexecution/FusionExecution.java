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

import fusion.hadoop.TextPair;
import fusion.hadoop.fusionkeycreation.FusionKeyMap;
import fusion.hadoop.fusionkeycreation.FusionKeyMapParser;
import fusion.hadoop.fusionkeycreation.MapFileParser;


public class FusionExecution {
	public static class FusionExecutionMapper
	extends Mapper<LongWritable, Text, TextPair, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		protected FusionKeyMapParser kmp;
		protected FusionKeyMap fkm;
		protected TextPair keyPairRaw = new TextPair(), keyPairFused = new TextPair();
		protected String empty = "";
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = new Configuration();
			Path[] paths = context.getLocalCacheFiles();
//			km.initialize(paths, conf);
//			km.initializeBySeqFile();
//			kmp = new FusionKeyMapParser();
//			fkm = new FusionKeyMap(kmp.getFusionKeyMap());
			fkm = new FusionKeyMap(new MapFileParser(conf));
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO: invoke input map class map method
			for (String mapKey : inputMap(key, value)) {
				keyPairRaw.set(mapKey, empty);
				context.write(keyPairRaw, one);
				fkm.assignFusedTextPair(mapKey, keyPairFused, empty);
				context.write(keyPairFused, one);
			}
		}
		
		protected Iterable<String> inputMap(LongWritable key, Text value) {
			ArrayList<String> keys = new ArrayList<String>();
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				keys.add(tokenizer.nextToken());
			}
			return keys;
		}
		
		protected IntWritable inputMapWrite(Text key) {
			return one;
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
			} else {
				fusedKey.set(key.toString());
				IntWritable value = inputReduce(fusedKey, values);
				if (value != null) {
					if (key.getFirst().toString().length() > 0) {
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
				//if (key.toString().compareTo("Hadoop,") == 0) throw new Exception("Fail to reduce.");
				for (IntWritable value : values) {
					sum += value.get();
				}
				return new IntWritable(sum);
			} catch (Exception ex) {
				System.err.println("Error when reducing key: " + key.toString() + "\n\t" + ex.getMessage());
			}
			return null;
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		main(args[0], args[1], args[2]);
	}

	protected static String FusionKeyPath;
	protected static int executeFusionExecutionJob(String inputPath, String outputPath, String fusionKeyPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		System.out.println("FusionExecutionCreation job begins");
		FusionKeyPath = fusionKeyPath;
		Configuration conf = new Configuration();
		//addFusionKeyCacheFiles(conf, fs, fusionKeyPath);
		Job job = Job.getInstance(conf);
		job.setJarByClass(FusionExecution.class);
		job.setJobName("FusionExecution");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(FusionExecutionMapper.class);
		job.setReducerClass(FusionExecutionReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setMaxReduceAttempts(1);	/// reduce retry execution to 1, relying on fusion for fault tolerance

		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("FusionExecution job ends with status " + status);
		return status;
	}

	protected static void addFusionKeyCacheFiles(Configuration job, FileSystem fs, String fusionKeyPath) throws IOException, URISyntaxException {
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

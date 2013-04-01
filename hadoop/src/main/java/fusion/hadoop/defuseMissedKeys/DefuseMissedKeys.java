package fusion.hadoop.defuseMissedKeys;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fusion.hadoop.TextPair;
import fusion.hadoop.fusionkeycreation.FusionKeyMapParser;

public class DefuseMissedKeys {

	public static class DefuseMapper1 extends DefuseMapper {
		public DefuseMapper1() {
			super(1);
		}
	}
	
	public static class DefuseMapper
	extends Mapper<LongWritable, Text, Text, ArrayWritable> {
		private Text word = new Text();
		protected int targetIndex = 0;

		private final static IntWritable empty = new IntWritable();
		protected FusionKeyMapParser km = new FusionKeyMapParser();
		protected TextPair keyPairRaw = new TextPair(), keyPairFused = new TextPair();
		protected ArrayWritable values = new ArrayWritable(IntWritable.class);
		
		public DefuseMapper(int targetIndex) {
			super();
			this.targetIndex = targetIndex; 
		}
		
		public DefuseMapper() {
			super();
		}
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = new Configuration();
			Path[] paths = context.getLocalCacheFiles();
			km.initialize(paths, conf);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] keys = value.toString().split("\t");
			IntWritable[] valueArray = new IntWritable[targetIndex + 1];
			int i=0;
			while (i<targetIndex) {
				valueArray[i] = new IntWritable();
				++i;
			}
			if (keys.length > 1) {
				String keyString = keys[0];
				System.out.println("\tEmptyTextMapper -- " + keys[0] + "\t " + keys[0].length());
				String missingKey = km.getOtherKeyForFusion(keyString);
				if (missingKey != null) {
					word.set(keyString);
					valueArray[targetIndex - 1] = new IntWritable(Integer.parseInt(keys[1]));
					values.set(valueArray);
					context.write(word, values);
				}
			}
		}
	}
	
	public static class DefuseReducer
	extends Reducer<Text, ArrayWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<ArrayWritable> values,
				Context context)
						throws IOException, InterruptedException {
			IntWritable fusedResult = null;
			IntWritable recoveryResult = null;
			for (ArrayWritable value : values) {
				if (value.get().length > 1) fusedResult = (IntWritable) value.get()[1];
				else recoveryResult = (IntWritable) value.get()[0];
				System.out.println("\t\tDefuseReducer: " + key + "\t" + value);
			}
			
			if (fusedResult != null && recoveryResult != null) {
				System.out.println("\trecovery keys: " + key + "\t" + key.toString().length());
				/// keys missing in result
				context.write(key, Defuse(fusedResult, recoveryResult));
			}
		}
	}
	
	public static IntWritable Defuse(IntWritable fusedResult, IntWritable recoveryResult) {
		IntWritable defuseResult = new IntWritable();
		defuseResult.set(fusedResult.get() - recoveryResult.get());
		return defuseResult;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		main(args[0], args[1], args[2], args[3]);
	}

	protected static void addMissedKeyCacheFiles(Configuration job, FileSystem fs, String missingKeyPath) throws IOException, URISyntaxException {
		String pattern = missingKeyPath + "/part-r-*";

		FileStatus[] fss = fs.globStatus(new Path(pattern));
		for (FileStatus fst : fss) {
			DistributedCache.addCacheFile(fst.getPath().toUri(), job);
			System.out.println("\tadding cache path: " + fst.getPath().toString());
		}
	}
	
	protected static int executeDefuseMissedKeysJob(String resultPath, String fusedKeyPath, String missingKeyPath, String missingKeyResultPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		System.out.println("DefuseMissedKey job begins");
		Configuration conf = new Configuration();
		Job job = Job.getInstance();
		job.setJarByClass(DefuseMissedKeys.class);
		job.setJobName("DefuseMissedKey");
		addMissedKeyCacheFiles(conf, fs, missingKeyPath);
		FileOutputFormat.setOutputPath(job, new Path(missingKeyResultPath));
		
		//MultipleInputs.addInputPath(job, new Path(missingKeyPath), TextInputFormat.class, keyPairMapper.class);
		MultipleInputs.addInputPath(job, new Path(resultPath), TextInputFormat.class, DefuseMapper.class);
		MultipleInputs.addInputPath(job, new Path(fusedKeyPath), TextInputFormat.class, DefuseMapper1.class);
		job.setReducerClass(DefuseReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("DefuseMissedKey job ends with status " + status);
		return status;
	}
	
	public static int main(String resultPath, String fusedKeyPath, String missingKeyPath, String missingKeyResultPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(missingKeyResultPath), true);

		int status = executeDefuseMissedKeysJob(resultPath, fusedKeyPath, missingKeyPath, missingKeyResultPath, fs);
		
		return status;
	}
}

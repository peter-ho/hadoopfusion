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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fusion.hadoop.TextPair;
import fusion.hadoop.fusionkeycreation.FusionKeyMap;
import fusion.hadoop.fusionkeycreation.FusionKeyMapParser;

public class DefuseMissedKeys {

	private static Class<? extends Writable> VALUE_CLASS = IntWritable.class;
	public static class DefuseMapper2_1 extends DefuseMapper {
		public DefuseMapper2_1() {
			super(2, 1);
		}
	}
	
	public static class DefuseMapper
	extends Mapper<LongWritable, Text, Text, DefuseArrayWritable> {
		private Text word = new Text();
		protected int sourceKeyCount = 1;
		protected int targetIndex = 0;

		private final static IntWritable empty = new IntWritable();
		protected FusionKeyMapParser km;
		protected FusionKeyMap fkm;
		protected TextPair keyPairRaw = new TextPair(), keyPairFused = new TextPair();
		protected DefuseArrayWritable values = new DefuseArrayWritable();
		
		public DefuseMapper(int sourceKeyCount, int targetIndex) {
			super();
			this.targetIndex = targetIndex; 
			this.sourceKeyCount = sourceKeyCount;
		}
		
		public DefuseMapper() {
			super();
		}
		
		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = new Configuration();
			Path[] paths = context.getLocalCacheFiles();
			km = new FusionKeyMapParser(paths, conf);
			fkm = new FusionKeyMap(km.getFusionKeyMap());
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
			if (keys.length > 1 && keys.length > sourceKeyCount) {
				String fusedResult = keys[sourceKeyCount];
				for (i=0; i<sourceKeyCount; ++i) {
					String keyString = keys[i];
					//System.out.println("\tDefuseMapper(" + targetIndex + ")-- " + keys[i] + "\t " + keys[i].length());
					String missingKey = fkm.getOtherKeyForFusion(keyString);
					if (missingKey != null) {
						word.set(missingKey);
						valueArray[targetIndex] = new IntWritable(Integer.parseInt(fusedResult));
						values.set(valueArray);
						context.write(word, values);
					}
				}
			}
		}
	}
	
	public static class DefuseReducer
	extends Reducer<Text, DefuseArrayWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<DefuseArrayWritable> values,
				Context context)
						throws IOException, InterruptedException {
			IntWritable fusedResult = null;
			IntWritable recoveryResult = null;
			for (DefuseArrayWritable value : values) {
				if (value.get().length > 1) fusedResult = (IntWritable) value.get()[1];
				else recoveryResult = (IntWritable) value.get()[0];
				//System.out.println("\t\tDefuseReducer: " + key + "\t" + value);
			}
			
			if (fusedResult != null && recoveryResult != null) {
				//System.out.println("\trecovery keys: " + key + "\t" + key.toString().length());
				/// keys missing in result
				context.write(key, Defuse(fusedResult, recoveryResult));
			}
		}
	}
	
	public static class DefuseArrayWritable extends ArrayWritable {
		public DefuseArrayWritable() {
			super(VALUE_CLASS);
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

	protected static int addMissedKeyCacheFiles(Configuration job, FileSystem fs, String missingKeyPath) throws IOException, URISyntaxException {
		String pattern = missingKeyPath + "/part-r-*";
		int fileCount = 0;

		FileStatus[] fss = fs.globStatus(new Path(pattern));
		for (FileStatus fst : fss) {
			if (fst.getLen() > 0) {
				DistributedCache.addCacheFile(fst.getPath().toUri(), job);
				System.out.println("\tadding cache path: " + fst.getPath().toString());
				++fileCount;
			}
		}
		return fileCount;
	}
	
	protected static int executeDefuseMissedKeysJob(String resultPath, String fusedResultPath, String missingKeyPath, String missingKeyResultPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		int status = 0;
		System.out.println("DefuseMissedKey job begins");
		Configuration conf = new Configuration();
		if (addMissedKeyCacheFiles(conf, fs, missingKeyPath) > 0) {
			Job job = Job.getInstance(conf);
			job.setJarByClass(DefuseMissedKeys.class);
			job.setJobName("DefuseMissedKey");
			FileOutputFormat.setOutputPath(job, new Path(missingKeyResultPath));
			
			//MultipleInputs.addInputPath(job, new Path(missingKeyPath), TextInputFormat.class, keyPairMapper.class);
			MultipleInputs.addInputPath(job, new Path(resultPath), TextInputFormat.class, DefuseMapper.class);
			MultipleInputs.addInputPath(job, new Path(fusedResultPath), TextInputFormat.class, DefuseMapper2_1.class);
			job.setReducerClass(DefuseReducer.class);
	
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DefuseArrayWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	
			status = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("DefuseMissedKey job ends with status " + status);
		} else {
			System.out.println("DefuseMissedKey job skipped due to empty missed key files.");
		}
		return status;
	}
	
	public static int main(String resultPath, String fusedResultPath, String missingKeyPath, String missingKeyResultPath) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(missingKeyResultPath), true);

		int status = executeDefuseMissedKeysJob(resultPath, fusedResultPath, missingKeyPath, missingKeyResultPath, fs);
		
		return status;
	}
}

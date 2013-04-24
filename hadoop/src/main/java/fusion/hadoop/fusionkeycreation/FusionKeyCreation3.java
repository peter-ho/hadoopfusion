package fusion.hadoop.fusionkeycreation;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import fusion.hadoop.WordCountFused;
import fusion.hadoop.fusionexecution.FusionExecution3;

public class FusionKeyCreation3 {
	protected static String JOB_NAME = "FusionKeyCreation3";
	//public static String FusionKeyPath = "hdfs://piccolo.saints.com:8020/user/peter/fusion/FusionKeyMap/fusionKey.seq";
	public static String FusionKeyPath = "hdfs://piccolo.saints.com:8020/user/peter/fusion/FusionKeyMap/part-r-00000";
	
	public static class FusionKeyMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> tokens = WordCountFused.WordCountMapper.map(value);
			FusionExecution3.FusionExecutionReducer.compute(value, null);
			for (String token : tokens) {
				word.set(token);
				context.write(word, inputMapper(word));
			}
		}
		
		public IntWritable inputMapper(Text key) 
		{
			return one;
		}
	}

	public static class FusionKeyReducer extends Reducer<Text, IntWritable, Text, FusionKeysWritable> {
		private Text last = new Text();
		protected IntWritable[] lastValues;
		private boolean lastConsumed = true;
		protected int count = 0;
		protected MultipleOutputs<Text, FusionKeysWritable> outputs;
		protected FusionKeysWritable fusionKeysWritable = new FusionKeysWritable();
		protected IntWritable[] typeWritableArray = new IntWritable[1];
		
		@Override
		protected void setup(Context context
				) throws IOException, InterruptedException {
			outputs = new MultipleOutputs<Text, FusionKeysWritable>(context);
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {
			if (lastConsumed) {
				last.set(key);
				lastValues = createWritableArray(values);
				lastConsumed = false;
			} else {
				lastConsumed = true;
				write(last, lastValues, key, createWritableArray(values));
				//context.write(last, key);
				//context.write(key, last);
				//multipleOutputs.write(key, new Text(last), "fusionkey");
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if (!lastConsumed) {
				write(last, lastValues);
				//context.write(last, last);
			}
			outputs.close();
		}

		protected void write(Text key, IntWritable[] values) throws IOException, InterruptedException {
//			keyCreationWritable.set(key);
//			outputs.write(key, keyCreationWritable, "fusionkey");

			fusionKeysWritable.Values.set(values);
			fusionKeysWritable.OtherKey = new Text();
			fusionKeysWritable.OtherKey.set("");
			fusionKeysWritable.OtherValues.set(new IntWritable[0]);
			outputs.write(key, fusionKeysWritable, "fusionkeyvalue");
			//outputs.write(key, keyCreationWritable, "key1");
		}
		
		protected void write(Text last, IntWritable[] lastValues, Text key, IntWritable[] values) throws IOException, InterruptedException {
//			keyCreationWritable.set(key);
//			outputs.write(last, keyCreationWritable, "fusionkey");

			fusionKeysWritable.Values.set(lastValues);
			fusionKeysWritable.OtherKey = key;
			fusionKeysWritable.OtherValues.set(values);
			outputs.write(last, fusionKeysWritable, "fusionkeyvalue");
//			write(last, lastValues, "key0");
//			write(key, values, "key1");
//			valueArrayWritable.set(createWritableArray(lastValues));
//			keyCreationWritable.set(valueArrayWritable);
//			outputs.write(last, keyCreationWritable, "key0");
//			
//			valueArrayWritable.set(createWritableArray(values));
//			keyCreationWritable.set(valueArrayWritable);
//			outputs.write(key, keyCreationWritable, "key1");
		}
		
//		protected void write(Text key, IntWritable[] values, String fileKey) throws IOException, InterruptedException {
//			valueArrayWritable.set(values);
//			keyCreationWritable.set(valueArrayWritable);
//			outputs.write(key, keyCreationWritable, fileKey);
//		}
		
		protected IntWritable[] createWritableArray(Iterable<IntWritable> values) {
			ArrayList<IntWritable> listValues = new ArrayList<IntWritable>();
			for (IntWritable value : values) {
				listValues.add(value);
			}
			IntWritable[] array = new IntWritable[listValues.size()];
			int i=0;
			for (IntWritable value : listValues) {
				array[i++] = value;
			}
			return array;
		}
	}


	
//	public static class KeyCreationWritable extends GenericWritable {
//
//		private static Class[] CLASSES = {
//			FusionKeysWritable.class,
//			Text.class
//		};
//
//		protected Class[] getTypes() {
//			return CLASSES;
//		}
//
//	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		main(args[0], args[1]);
	}

	protected static int executeFusionKeyCreationJob(String inputPath, String outputPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println(JOB_NAME + " job begins");
		Job job = Job.getInstance();
		job.setJarByClass(FusionKeyCreation.class);
		job.setJobName(JOB_NAME);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(FusionKeyMapper.class);
		job.setReducerClass(FusionKeyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FusionKeysWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		MapFileOutputFormat.setCompressOutput(job, true);
//		MapFileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);

		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(JOB_NAME + " job ends with status " + status);
		fs.delete(new Path(outputPath + "/_SUCCESS"), true);
		fs.delete(new Path(outputPath + "/_logs"), true);
		return status;
	}
	
	public static int main(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);

		int status = executeFusionKeyCreationJob(inputPath, outputPath, fs);
		
		return status;
	}

}

package fusion.hadoop.missedKeySearch;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fusion.hadoop.fusionkeycreation.FusionKeyCreation;

public class MissedKeySearch {

	public static class EmptyTextMapper
	extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private final static Text emptyText = new Text("");

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] keys = value.toString().split("\t");
			if (keys.length > 0) {
				word.set(keys[0]);
				context.write(word, emptyText);
			}
		}
	}
	
	public static class keyPairMapper 
	extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text key1 = new Text(), key2 = new Text(), value1 = new Text(), value2 = new Text();
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] keys = value.toString().split("\t");
			if (keys.length > 0) {
				key1.set(keys[0]);
				value1.set("R" + keys[1]);
				key2.set(keys[1]);
				value2.set("R" + keys[0]);
				context.write(key1, value1);
				context.write(key2, value2);
			} else {
				key1.set(keys[0]);
				value1.set("S" + keys[0]);
				context.write(key1, value1);
			}
		}
	}

	public static class MissedKeySearchReducer
	extends Reducer<Text, Text, Text, Text> {

		private Text recoveryKeyText = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context)
						throws IOException, InterruptedException {
			boolean emptyExists = false;
			String recoveryKey = null;
			for (Text value : values) {
				String keyString = value.toString();
				if (keyString.length() == 0) emptyExists = true;
				else recoveryKey = keyString.substring(1);
			}
			
			if (!emptyExists && recoveryKey != null) {
				/// keys missing in result
				recoveryKeyText.set(recoveryKey);
				context.write(recoveryKeyText, key);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		main(args[0], args[1], args[2]);
	}

	
	protected static int executeMissedKeySearchJob(String resultPath, String fusedKeyPath, String outputPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException {

		System.out.println("FusionKeyCreation job begins");
		Job job = Job.getInstance();
		job.setJarByClass(FusionKeyCreation.class);
		job.setJobName("FusionKeyCreation");

		//FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		MultipleInputs.addInputPath(job, new Path(resultPath), TextInputFormat.class, EmptyTextMapper.class);
		MultipleInputs.addInputPath(job, new Path(fusedKeyPath), TextInputFormat.class, keyPairMapper.class);
		//job.setMapperClass(MissedKeySearchMapper.class);
		job.setReducerClass(MissedKeySearchReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("FusionKeyCreation job ends with status " + status);
		return status;
	}
	
	public static int main(String resultPath, String fusedKeyPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{
//		String tempOutputPath = outputPath + "Output";
		
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);

		int status = executeMissedKeySearchJob(resultPath, fusedKeyPath, outputPath, fs);
		
		return status;
	}
}

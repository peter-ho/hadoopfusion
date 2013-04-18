package fusion.hadoop.fusionkeycreation;


import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import fusion.hadoop.WordCountFused;

public class FusionKeyCreateToSeqFile {
	protected static String JOB_NAME = "FusionKeyCreateToSeqFile";
	//public static String FusionKeyPath = "hdfs://piccolo.saints.com:8020/user/peter/fusion/FusionKeyMap/fusionKey.seq";
	public static String FusionKeyPath = "hdfs://piccolo.saints.com:8020/user/peter/fusion/FusionKeyMap/part-r-00000";
	
	public static class FusionKeyMapper
	extends Mapper<LongWritable, Text, Text, NullWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> tokens = WordCountFused.WordCountMapper.map(value);
			for (String token : tokens) {
				word.set(token);
				context.write(word, NullWritable.get());
			}
		}
	}

	public static class FusionKeyReducer extends Reducer<Text, NullWritable, Text, Text> {
		private Text last = new Text();
		private boolean lastConsumed = true;
		protected int count = 0;
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context)
						throws IOException, InterruptedException {
			if (lastConsumed) {
				last.set(key);
				lastConsumed = false;
			} else {
				lastConsumed = true;
				context.write(last, key);
				//context.write(key, last);
				count += 2;
				//multipleOutputs.write(key, new Text(last), "fusionkey");
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			if (!lastConsumed) {
				context.write(last, new Text(""));
				++count;
			}
			System.out.println("\t *** key count: " + count);
		}
	}
	
	public static class FusionKeyReducerExplicitSequenceFile
	extends Reducer<Text, NullWritable, Text, Text> {

		private Text last = new Text();
		private boolean lastConsumed = true;
		SequenceFile.Writer writer = null;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
//			FileSystem fs = FileSystem.get(conf);
			String path = FusionKeyPath;
			System.out.println("*** path: " + path);
			writer = SequenceFile.createWriter(conf, Writer.compression(CompressionType.NONE), Writer.file(new Path(path)), 
					Writer.keyClass(Text.class), Writer.valueClass(Text.class));
			//writer = SequenceFile.createWriter(fs, conf, new Path(path), Text.class, Text.class, CompressionType.NONE);
		}
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context)
						throws IOException, InterruptedException {
			System.out.println("reducing: " + key.toString());
			if (lastConsumed) {
				last.set(key);
				lastConsumed = false;
			} else {
				lastConsumed = true;
				//context.write(key, new Text(last));
				//multipleOutputs.write(key, new Text(last), "fusionkey");
				try {
					writer.append(key, last);
				} finally {
					//IOUtils.closeStream(writer);
				}
			}
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			try {
				if (!lastConsumed) writer.append(last, new Text(""));
			} finally {
				IOUtils.closeStream(writer);
			}
		}
	}
	
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
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(1);
		job.setOutputFormatClass(MapFileOutputFormat.class);
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

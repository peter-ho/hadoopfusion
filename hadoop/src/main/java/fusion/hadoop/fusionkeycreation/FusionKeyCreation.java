package fusion.hadoop.fusionkeycreation;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;


// https://ccp.cloudera.com/display/CDH4DOC/Using+the+CDH4+Maven+Repository
public class FusionKeyCreation {
	public static class FusionKeyMapper
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

	public static class FusionKeyReducer
	extends Reducer<Text, NullWritable, Text, Text> {

		private Text last = new Text();
		private MultipleOutputs<Text, Text> multipleOutputs;
		private boolean lastConsumed = true;
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs(context);
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
		main(args[0], args[1]);
	}
		
	public static int main(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{
		System.out.println("FusionKeyCreation job begins");
		
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(new Configuration());
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);

		Job job = Job.getInstance();
		job.setJarByClass(FusionKeyCreation.class);
		job.setJobName("FusionKeyCreation");

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(FusionKeyMapper.class);
		job.setReducerClass(FusionKeyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		int status = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("FusionKeyCreation job ends");
		
		System.out.println("Handle remainder starts");
		//fs.open("");
		System.out.println("Handle remainder ends");		
		return status;
	}
}

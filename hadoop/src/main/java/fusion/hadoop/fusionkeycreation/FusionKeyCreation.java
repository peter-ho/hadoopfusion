package fusion.hadoop.fusionkeycreation;

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
		main(args[0], args[1]);
	}

	protected static int executeFusionKeyCreationJob(String inputPath, String outputPath, FileSystem fs) throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println("FusionKeyCreation job begins");
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
		System.out.println("FusionKeyCreation job ends with status " + status);
		return status;
	}

	public static ArrayList<String[]> parseRemainderFiles(String outputPath, FileSystem fs) throws IOException {
		ArrayList<String[]> remainderKeys = new ArrayList<String[]>();
		String[] keys;
		System.out.println("Handle remainder starts");
		FileStatus[] fss = fs.globStatus(new Path(outputPath + "/remainder-r-*"));
		String last = null;
		for (FileStatus fst : fss) {
			FSDataInputStream in = fs.open(fst.getPath());
			String line = in.readLine();
			while (line != null) {
				if (last == null) last = line;
				else {
					remainderKeys.add(new String[] { line, last});
					last = null;
				}
				line = in.readLine();
			}
			in.close();
		}
		if (last != null) remainderKeys.add(new String[] { last });
		System.out.println("Handle remainder ends");
		return remainderKeys;
	}
	
	protected static void saveRemainder(ArrayList<String[]> remainders, FileSystem fs, String outputPath) throws IOException {
		if (remainders.size() > 0) {
			System.out.println("Save remainder starts");
			FSDataOutputStream outputStrm = fs.append(new Path(outputPath + "/fusionkey-r-00000" ));
			for (String[] keys : remainders) {
				System.out.println("\tStart writing remainder " + keys[0]);
				if (keys.length > 1) {
					outputStrm.writeChars(keys[0]);
					outputStrm.write('\t');
					outputStrm.writeChars(keys[1]);
					outputStrm.write('\r');
					outputStrm.write('\n');
				} else {
					outputStrm.writeChars(keys[0]);
					outputStrm.write('\r');
					outputStrm.write('\n');
				}
			}
			outputStrm.close();
			System.out.println("Save remainder starts");
		}
	}
	
	public static int main(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException
	{
//		String tempOutputPath = outputPath + "Output";
		
		Configuration conf = new Configuration();
		// configuration should contain reference to your namenode
		FileSystem fs = FileSystem.get(conf);
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(outputPath), true);

		int status = executeFusionKeyCreationJob(inputPath, outputPath, fs);
		
		if (status == 0) {
			/// handle remainder
			ArrayList<String[]> remainders = parseRemainderFiles(outputPath, fs);
			saveRemainder(remainders, fs, outputPath);
		}
		return status;
	}

}

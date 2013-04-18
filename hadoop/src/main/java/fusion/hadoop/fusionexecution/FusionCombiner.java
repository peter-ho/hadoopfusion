package fusion.hadoop.fusionexecution;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.ReflectionUtils;

import fusion.hadoop.TextPair;
import fusion.hadoop.fusionkeycreation.MapFileParser;

public class FusionCombiner extends
		Reducer<TextPair, IntWritable, TextPair, IntWritable> {

	private Text fusedKey = new Text();
	private MultipleOutputs<Text, IntWritable> multipleOutputs;
	private Reader reader;
	Configuration conf;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path (MapFileParser.PATH + "/part-r-*/data");
		reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
	}
	
	@Override
	public void reduce(TextPair key, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {
		String strKey = getKey(key);
		
		Writable key1 = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable key2 = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		while (reader.next(key1, key2)) {
			if (key2 != null && key2.toString() != null && !key2.toString().isEmpty()) {
				/// key pair exists
			}
		}
	}
	
	protected String getKey(TextPair key) {
		String first = key.getFirst().toString();
		return (first == null || first.isEmpty()) ? key.getSecond().toString() : first;
	}
		
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		reader.close();
	}
}
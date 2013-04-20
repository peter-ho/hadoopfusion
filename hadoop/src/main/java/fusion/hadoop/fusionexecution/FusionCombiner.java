package fusion.hadoop.fusionexecution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

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
	Writable key1, key2;
	Text empty;
	boolean moreData = true;
	TextPair tp = new TextPair();
	Collection<IntWritable> lastValues = null;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path (MapFileParser.PATH + "/part-r-00000/data");
		reader = new SequenceFile.Reader(fs, path, context.getConfiguration());
		key1 = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		key2 = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		empty = new Text("");
		moreData = reader.next(key1, key2);
		System.out.println("  read from key file " + key1.toString() + "\t" + key2.toString());
	}
	
	@Override
	public void reduce(TextPair key, Iterable<IntWritable> values,
			Context context)
					throws IOException, InterruptedException {
		String strKey = key.getFirst().toString();
		System.out.println("  combine key: " + strKey);
		ArrayList<IntWritable> valueCopy = new ArrayList<IntWritable>();
		for (IntWritable value : values) {
			valueCopy.add(value);
		}
 
		if (!strKey.isEmpty()) {
			while (!key2.toString().isEmpty() && strKey.compareTo(key2.toString()) > 0) {
				/// write last pending fused key
				if (lastValues != null) {
					writeToContext(tp, lastValues, context);
					lastValues = null;
				}
				reader.next(key1, key2);
				System.out.println("  read from key file " + key1.toString() + "\t" + key2.toString());
			}
			if (strKey.compareTo(key1.toString()) == 0) {
				tp.set(key1.toString(), key2.toString());
				writeToContext(key, valueCopy, context);
				lastValues = valueCopy;
			} else if (strKey.compareTo(key2.toString()) == 0) {
				key.set("", strKey);
				writeToContext(key, valueCopy, context);
				if (lastValues != null) {
					writeToContext(tp, lastValues, context);
					lastValues = null;
				}
				key.set(key1.toString(), key2.toString());
				writeToContext(key, valueCopy, context);
				moreData = reader.next(key1, key2);
				System.out.println("  read from key file " + key1.toString() + "\t" + key2.toString());
			}
		}
	}
	
	protected void writeToContext(TextPair key, Collection<IntWritable> values, Context context) throws IOException, InterruptedException {
		for (IntWritable value : values) {
			context.write(key, value);
			System.out.println("   wrote : " + key.toString() + " :: " + value.toString());
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
		if (lastValues != null) {
			if (key2 == null || key2.toString() == null || key2.toString().isEmpty()) key2 = key1;
			tp.set(key1.toString(), key2.toString());
			writeToContext(tp, lastValues, context);
			lastValues = null;
		}
	}
}
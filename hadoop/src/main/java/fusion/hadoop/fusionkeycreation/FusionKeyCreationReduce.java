package fusion.hadoop.fusionkeycreation;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FusionKeyCreationReduce<Text, NullWritable> extends
		Reducer<Text, NullWritable, Text, Text> {

	protected Text m_last = null;
	
	@Override
	public void reduce(Text key, Iterable<NullWritable> values,
			Context context)
					throws IOException, InterruptedException {

		if (m_last == null) m_last = key;
		else {
			context.write(m_last, key);
			context.write(key, m_last);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (m_last != null) {
		}
	}
}
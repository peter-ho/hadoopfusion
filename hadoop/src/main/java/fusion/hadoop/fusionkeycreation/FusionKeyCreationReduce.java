package fusion.hadoop.fusionkeycreation;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FusionKeyCreationReduce extends
		Reducer<Text, NullWritable, Text, Text> {

	protected Text m_last = new Text("");
	
	@Override
	public void reduce(Text key, Iterable<NullWritable> values,
			Context context)
					throws IOException, InterruptedException {

		context.write(key, new Text(m_last));
		m_last = key;
		/*
		if (m_last == null) {
			m_last = key;
		}
		else {
			//context.write(m_last, key);
			context.write(key, m_last);
		}
		*/
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (m_last != null) {
			context.write(m_last, new Text(""));
		}
	}
}
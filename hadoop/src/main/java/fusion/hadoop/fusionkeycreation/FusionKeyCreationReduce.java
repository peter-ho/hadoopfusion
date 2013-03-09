package fusion.hadoop.fusionkeycreation;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class FusionKeyCreationReduce<KEYIN, VALUEIN> extends
		Reducer<KEYIN, VALUEIN, KEYIN, KEYIN> {

	protected KEYIN m_last = null;
	
	@Override
	public void reduce(KEYIN key, Iterable<VALUEIN> values,
			Context context)
					throws IOException, InterruptedException {

		if (m_last == null) m_last = key;
		else {
			context.write(m_last, key);
			context.write(key, m_last);
		}
	}
}

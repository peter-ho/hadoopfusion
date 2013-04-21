package fusion.hadoop.fusionkeycreation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FusionKeysWritable implements Writable {

	public Text OtherKey = new Text();
	public ValueArrayWritable Values = new ValueArrayWritable();
	public ValueArrayWritable OtherValues = new ValueArrayWritable();
	
	public void write(DataOutput out) throws IOException {
		OtherKey.write(out);
		Values.write(out);
		OtherValues.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		OtherKey.readFields(in);
		Values.readFields(in);
		OtherValues.readFields(in);
	}

	public static class ValueArrayWritable extends ArrayWritable {

		public ValueArrayWritable() {
			super(IntWritable.class);
		}
	}	
}


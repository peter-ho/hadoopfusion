package fusion.hadoop;

import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair() {
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public void set(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		if (first == null) first = new Text();
		if (second == null) second = new Text();
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair ip = (TextPair) o;
			return first == ip.first && second == ip.second;
		}
		return false;
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}

	protected Text getLower() {
		return (compare(first, second) > 0)? second : first;
	}
	
	protected Text getHigher() {
		return (compare(first, second) > 0)? first : second;
	}
	
	/**
	 * Convenience method for comparing two ints.
	 */
	public static int compare(Text a, Text b) {
		return a.compareTo(b);
	}

	public int compareTo(TextPair tp) {
		int cmp = compare(getLower(), tp.getLower());
		if (cmp != 0) {
			return cmp;
		}
		return compare(getHigher(), tp.getHigher());
	}

}

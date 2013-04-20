package fusion.hadoop;

import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

	private Text first;
	private Text second;

	public TextPair() {
		first = new Text();
		second = new Text();
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
	
	public boolean isFirstEmpty() {
		String strFirst = first.toString();
		return strFirst == null || strFirst.isEmpty();
	}
	
	public boolean isSecondEmpty() { 
		String strSecond = second.toString();
		return strSecond == null || strSecond.isEmpty();
	}
	
	public String getSingleValue() {
		boolean firstEmpty = isFirstEmpty(), secondEmpty = isSecondEmpty();
		if (firstEmpty && !secondEmpty) return second.toString();
		if (!firstEmpty && secondEmpty) return first.toString();
		return null;
	}
	
	public int compareTo(TextPair tp) {
		String singleValue = getSingleValue();
		String otherSingleValue = tp.getSingleValue();
		if (singleValue != null && otherSingleValue != null) return singleValue.compareTo(otherSingleValue);
		String higher = getHigher().toString(), lower = getLower().toString();
		if (singleValue == null && otherSingleValue != null) {
			if (lower.compareTo(otherSingleValue) < 0) return -1;
			return 1;
		}
		if (singleValue != null && otherSingleValue == null) {
			if (tp.getHigher().toString().compareTo(singleValue) >= 0) return -1;
			else return 1;
		}
		return compareToBoth(tp);
	}

	public int compareToBoth(TextPair tp) {
		int cmp = compare(first, tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return compare(second, tp.second);
	}

}

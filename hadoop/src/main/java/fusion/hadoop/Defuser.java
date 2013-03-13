package fusion.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//public interface Defuser<K,V> {
public interface Defuser {
	//V defuse(V fusedResult, V parentResult, K fusedKey, K parentKey, K missingKey);
	IntWritable defuse(IntWritable fusedResult, IntWritable parentResult, Text fusedKey, Text parentKey, Text missingKey);
}
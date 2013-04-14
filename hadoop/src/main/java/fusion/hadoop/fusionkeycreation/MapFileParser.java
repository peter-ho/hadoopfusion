package fusion.hadoop.fusionkeycreation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;

import fusion.hadoop.TextPair;

public class MapFileParser implements Map<String, String> {

	public static String PATH = "hdfs://piccolo.saints.com:8020/user/peter/fusion/FusionKeyMap";
	protected Reader[] readers;
	protected Partitioner<Text, Text> partitioner;
	
	public MapFileParser(Configuration conf) throws IOException {
		//m_path = path;
		readers = MapFileOutputFormat.getReaders(new Path(PATH), conf);
		partitioner = new HashPartitioner<Text, Text>();
//		MapFileOutputFormat.getEntry(readers, partitioner, key, value)
	}

	public void clear() {
		// TODO Auto-generated method stub
	}

	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	public Set<java.util.Map.Entry<String, String>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public String get(Object key) {
		Text textKey = new Text(), textValue = new Text();
		textKey.set(key.toString());
		Writable txtOutput;
		try {
			txtOutput = MapFileOutputFormat.getEntry(readers, partitioner, textKey, textValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return txtOutput == null? null : ((Text) txtOutput).toString();
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public Set<String> keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public void putAll(Map<? extends String, ? extends String> m) {
		// TODO Auto-generated method stub
		
	}

	public String remove(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public Collection<String> values() {
		// TODO Auto-generated method stub
		return null;
	}

	public String put(String key, String value) {
		// TODO Auto-generated method stub
		return null;
	}
}

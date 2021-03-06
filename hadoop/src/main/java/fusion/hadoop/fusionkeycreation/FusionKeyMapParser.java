package fusion.hadoop.fusionkeycreation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import fusion.hadoop.TextPair;

public class FusionKeyMapParser {

	private Map<String, String> fusionKeyMap = new HashMap<String, String>();

	public FusionKeyMapParser() throws IOException {
		String uri = FusionKeyCreateToSeqFile.FusionKeyPath;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)
			ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)
			ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				if (value != null && value.toString() != null && !value.toString().isEmpty()) {
					fusionKeyMap.put(key.toString(), value.toString());
					fusionKeyMap.put(value.toString(), key.toString());
				}
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	public FusionKeyMapParser(Path[] paths, Configuration conf) throws IOException {
		String[] keys;
		FSDataInputStream in = null;
		BufferedReader br = null;
//		HashMap<String, String> remainderMap = new HashMap<String, String>();
		System.out.println("fusion key map building starts");

//		String last = null;
		for (Path path : paths) {
			try {
				//in = new FSDataInputStream(fs.open(path));
				FileSystem fsLocal = FileSystem.getLocal(conf);
				in = new FSDataInputStream(fsLocal.open(path));
				br = new BufferedReader(new InputStreamReader(in));

				String line = br.readLine();
				System.out.println("\tbuilding: " + path.getName() + " - " + line);
				//if (path.getName().indexOf("fusionkey") >= 0) {
					while (line != null && line.length() > 0 ) {
						keys = line.split("\t");
						if (keys.length > 1) {
							// System.out.println("\tadding " + keys[0] + ", " + keys[1]);
							fusionKeyMap.put(keys[0], keys[1]);
							fusionKeyMap.put(keys[1], keys[0]);
						}
						// ignore invalid entries without key pairs
						line = br.readLine();
					}
				//} 
//				else if (path.getName().indexOf("remainder") >= 0) {
//					while (line != null && line.length() > 0) {
//						if (last == null) last = line;
//						else {
//							System.out.println("\tadding " + line + ", " + last);
//							fusionKeyMap.put(line, last);
//							fusionKeyMap.put(last, line);
//							remainderMap.put(line, last);
//							last = null;
//						}
//						// ignore invalid entries without key pairs
//						line = br.readLine();
//					}
//				}
			} finally {
				if (br != null) IOUtils.closeStream(br);
				if (in != null) IOUtils.closeStream(in);
			}
		}

		System.out.println("fusion key map building ends");
	}
	
//	public String getOtherKeyForFusion(String key) {
//		if (fusionKeyMap.containsKey(key)) {
//			return fusionKeyMap.get(key);
//		} else
//			return null;
//	}
//	
//	public String getFusedKey(String key) {
//		String otherKey = fusionKeyMap.get(key);
//		String fusedKey = null;
//		if (otherKey != null) {
//			if (otherKey.compareTo(key) > 0) fusedKey = key.concat(otherKey);
//			else fusedKey = otherKey.concat(key);
//		} 
//		return fusedKey;
//	}

	public Map<String, String> getFusionKeyMap() {
		return Collections.unmodifiableMap(fusionKeyMap);
	}
	
//	public void assignFusedTextPair(String key, TextPair textPair, String otherKeyReplacement) {
//		if (fusionKeyMap.containsKey(key)) {
//			String otherKey = fusionKeyMap.get(key);
//			if (key.compareTo(otherKey) < 0) textPair.set(key, otherKey);
//			else textPair.set(otherKey,  key);
//		} else {
//			textPair.set(otherKeyReplacement, key);
//		}
//	}
}

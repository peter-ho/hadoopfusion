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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FusionKeyMapParser {

	private Map<String, String> fusionKeyMap = new HashMap<String, String>();

	public void initialize(URI[] uris, FileSystem fs) throws IOException {
		String[] keys;
		FSDataInputStream in = null;
		BufferedReader br = null;
		System.out.println("fusion key map building starts");

		String last = null;
		for (URI uri : uris) {
			try {
				in = new FSDataInputStream(new FileInputStream(new File(uri)));
				br = new BufferedReader(new InputStreamReader(in));

				String line = br.readLine();
				if (uri.getPath().indexOf("fusionkey") > 0) {		
					while (line != null) {
						keys = line.split("\t");
						System.out.println("\tadding " + keys[0] + ", " + keys[1]);
						if (keys.length > 1) fusionKeyMap.put(keys[0], keys[1]);
						// ignore invalid entries without key pairs
						line = br.readLine();
					}
				} else if (uri.getPath().indexOf("remainder") > 0) {
					while (line != null && line.length() > 0) {
						if (last == null) last = line;
						else {
							System.out.println("\tadding " + line + ", " + last);
							fusionKeyMap.put(line, last);
							fusionKeyMap.put(last, line);
							last = null;
						}
						// ignore invalid entries without key pairs
						line = br.readLine();
					}
				}
			} finally {
				if (br != null) IOUtils.closeStream(br);
				if (in != null) IOUtils.closeStream(in);
			}
		}
		
		
//		FileStatus[] fss = fs.globStatus(new Path(fusionKeyPath + "/fusionkey-r-*"));
//		for (FileStatus fst : fss) {
//			try {
//				in = fs.open(fst.getPath());
//				br = new BufferedReader(new InputStreamReader(in));
//	
//				String line = br.readLine();
//				while (line != null) {
//					keys = line.split("\t");
//					System.out.println("\tadding " + keys[0] + ", " + keys[1]);
//					if (keys.length > 1) fusionKeyMap.put(keys[0], keys[1]);
//					// ignore invalid entries without key pairs
//					line = br.readLine();
//				}
//			} finally {
//				if (br != null) IOUtils.closeStream(br);
//				if (in != null) IOUtils.closeStream(in);
//			}
//		}
//
//		System.out.println("\tfusion key map remainder building starts");
//		fss = fs.globStatus(new Path(fusionKeyPath + "/remainder-r-*"));
//		String last = null;
//		for (FileStatus fst : fss) {
//			try {
//				in = fs.open(fst.getPath());
//				br = new BufferedReader(new InputStreamReader(in));
//	
//				String line = br.readLine();
//				while (line != null && line.length() > 0) {
//					if (last == null) last = line;
//					else {
//						System.out.println("\tadding " + line + ", " + last);
//						fusionKeyMap.put(line, last);
//						fusionKeyMap.put(last, line);
//						last = null;
//					}
//					// ignore invalid entries without key pairs
//					line = br.readLine();
//				}
//			} finally {
//				if (br != null) IOUtils.closeStream(br);
//				if (in != null) IOUtils.closeStream(in);
//			}
//		}

		System.out.println("fusion key map building ends");
	}
	
	public String getOtherKeyForFusion(String key) {
		if (fusionKeyMap.containsKey(key)) {
			return fusionKeyMap.get(key);
		} else
			return null;
	}

	public Map<String, String> getFusionKeyMap() {
		return Collections.unmodifiableMap(fusionKeyMap);
	}
}

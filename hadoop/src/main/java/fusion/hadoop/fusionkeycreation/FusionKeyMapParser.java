package fusion.hadoop.fusionkeycreation;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;

public class FusionKeyMapParser {

	private Map<String, String> fusionKeyMap = new HashMap<String, String>();

	protected static void saveRemainder(ArrayList<String[]> remainders,
			FileSystem fs, String tempOutputPath) throws IOException {
		if (remainders.size() > 0) {
			FSDataOutputStream outputStrm = fs.append(new Path(tempOutputPath
					+ "/fusionkey-r-00000"));
			for (String[] keys : remainders) {
				if (keys.length > 1) {
					outputStrm.writeChars(keys[0]);
					outputStrm.write('\t');
					outputStrm.writeChars(keys[1]);
					outputStrm.write('\r');
					outputStrm.write('\n');
					outputStrm.writeChars(keys[1]);
					outputStrm.write('\t');
					outputStrm.writeChars(keys[0]);
					outputStrm.write('\r');
					outputStrm.write('\n');
				} else {
					outputStrm.writeChars(keys[0]);
					outputStrm.write('\r');
					outputStrm.write('\n');
				}
			}
			outputStrm.close();
		}
	}

	public void initialize(FileSystem fs, String fusionKeyPath) throws IOException {
		ArrayList<String[]> remainderKeys = new ArrayList<String[]>();
		String[] keys;
		System.out.println("fusion key map building starts");
		FileStatus[] fss = fs.globStatus(new Path(fusionKeyPath
				+ "/remainder-r-*"));
		String last = null;
		for (FileStatus fst : fss) {
			FSDataInputStream in = fs.open(fst.getPath());
			String line = in.readLine();
			while (line != null) {
				if (last == null)
					last = line;
				else {
					remainderKeys.add(new String[] { line, last });
					last = null;
				}
				line = in.readLine();
			}
			in.close();
		}
		if (last != null)
			remainderKeys.add(new String[] { last });

		
		
		
		
//		BufferedReader in = null;
//		try {
//			in = new BufferedReader(new InputStreamReader(new FileInputStream(
//					file)));
//			String line;
//			while ((line = in.readLine()) != null) {
//				String[] keys = line.split("\t");
//				if (keys.length > 1)
//					fusionKeyMap.put(keys[0], keys[1]);
//				// / TODO: determine whether keys missing counterpart is
//				// required to be present in the map
//				// / else if (keys.length > 0) fusionKeyMap.put(keys[0],
//				// keys[0]);
//			}
//		} finally {
//			IOUtils.closeStream(in);
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

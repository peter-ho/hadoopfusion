package fusion.hadoop.fusionkeycreation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import fusion.hadoop.TextPair;

public class FusionKeyMap {

	private Map<String, String> fusionKeyMap;// = new HashMap<String, String>();

	public FusionKeyMap(Map<String, String> data) {
		fusionKeyMap = data;
	}
	
	public String getOtherKeyForFusion(String key) {
		String value = fusionKeyMap.get(key);
		System.out.println("key: " + key + " value: " + value);
		return value;
//		if (fusionKeyMap.containsKey(key)) {
//			return fusionKeyMap.get(key);
//		} else
//			return null;
	}
	
//	public String getFusedKey(String key) {
//		String otherKey = fusionKeyMap.get(key);
//		String fusedKey = null;
//		if (otherKey != null) {
//			if (otherKey.compareTo(key) > 0) fusedKey = key.concat(otherKey);
//			else fusedKey = otherKey.concat(key);
//		} 
//		return fusedKey;
//	}

//	public Map<String, String> getFusionKeyMap() {
//		return Collections.unmodifiableMap(fusionKeyMap);
//	}
	
	public void assignFusedTextPair(String key, TextPair textPair, String otherKeyReplacement) {
		String otherKey = fusionKeyMap.get(key);
		if (otherKey != null && !otherKey.isEmpty()) {
			System.out.println("key: " + key + " value: " + otherKey);
			if (key.compareTo(otherKey) < 0) textPair.set(key, otherKey);
			else textPair.set(otherKey,  key);
		} else {
			textPair.set(otherKeyReplacement, key);
		}
	}
}

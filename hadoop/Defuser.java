package fusion.hadoop;

public interface Defuser<K,V> {
  V defuse(V fusedResult, V parentResult, K fusedKey, K parentKey, K missingKey);
}
package fusion.hadoop.fusionexecution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import fusion.hadoop.TextPair;

public class FusionPartitioner extends Partitioner<TextPair, IntWritable> {

	protected static int BUCKET_COUNT = 3;
	protected int[] bucketSize;
	protected int[] bucketOffst;
	
	public FusionPartitioner(int numPartitions) {
		bucketSize = createBucketSize(numPartitions);
		bucketOffst = createBucketOffset(bucketSize);
	}
	
	@Override
	public int getPartition(TextPair key, IntWritable value, int numPartitions) {
		int bucketIdx = getBucketIdx(key);
		return getAssignedPartition(key, bucketIdx);
	}

	protected int getAssignedPartition(TextPair key, int bucketIdx) {
		int size = bucketSize[bucketIdx];
		return bucketOffst[bucketIdx] + ((key.getFirst().hashCode() % size) + (key.getSecond().hashCode() % size)) % size;
	}

	protected int getBucketIdx(TextPair key) {
		String first = key.getFirst().toString(), second = key.getSecond().toString();
		if (first.length() > 0 && second.length() > 0) return 0;
		return (first.length() > 0)? 1 : 2;
	}

	protected int[] createBucketOffset(int[] bucketSize) {
		int[] bucketOffset = new int[BUCKET_COUNT];
		bucketOffset[0] = 0;
		for (int i=0; i<BUCKET_COUNT-1; ++i) {
			bucketOffset[i+1] = bucketSize[i] + bucketOffset[i];
		}
		return bucketOffset;
	}

	protected int[] createBucketSize(int numPartitions) {
		int[] bucketSizes = new int[BUCKET_COUNT];
		int averageSize = numPartitions / BUCKET_COUNT;
		bucketSizes[0] = averageSize;
		bucketSizes[1] = averageSize;
		bucketSizes[2] = averageSize;
		int remainder = numPartitions % BUCKET_COUNT;
		for (int i=1, j=0; i<=remainder; ++i, j=(j+1)%BUCKET_COUNT) {
			++bucketSizes[j];
		}
		return bucketSizes;
	}
}

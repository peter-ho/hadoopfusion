package fusion.hadoop;

import fusion.hadoop.fusionexecution.FusionPartitioner;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class FusionPartitionerTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public FusionPartitionerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( FusionPartitionerTest.class );
    }

    public void testPartitionSize3()
    {
    	int numPartition = 3;
    	FusionPartitioner fp = new FusionPartitioner(numPartition);
    	TextPair key = new TextPair();
    	key.set("abc", "def");
    	assertEquals(0, fp.getPartition(key, null, numPartition));
    	key.set("abc", "");
    	assertEquals(1, fp.getPartition(key, null, numPartition));
    	key.set("", "aaa");
    	assertEquals(2, fp.getPartition(key, null, numPartition));
    }
    
    public void testPartitionSize6()
    {
    	int numPartition = 6;
    	FusionPartitioner fp = new FusionPartitioner(numPartition);
    	TextPair key = new TextPair();
    	key.set("abc", "def");
    	assertEquals("abc".hashCode(), "abc".hashCode());
    	assertEquals(0, "abc".hashCode() % 2);
    	assertEquals(1, "def".hashCode() % 2);    	
    	assertEquals(1, fp.getPartition(key, null, numPartition));
    	
    	key.set("abc", "");
    	assertEquals(0, "".hashCode() % 2);
    	assertEquals(2, fp.getPartition(key, null, numPartition));
    	key.set("", "aaa");
    	assertEquals(1, "aaa".hashCode() % 2);
    	assertEquals(5, fp.getPartition(key, null, numPartition));
    }

}

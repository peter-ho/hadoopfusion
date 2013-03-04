package fusion.hadoop;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import fusion.hadoop.FusionJobConf;
import fusion.hadoop.*;
import fusion.hadoop.fusionkeycreation.*;


public class FusionJobClient {
 protected const String FUSION_PATH = "fusion_temp";
 protected static String initTempStorage(String jobId) {
  File fusionPath = new File(FUSION_PATH);
  File jobPath = new File(fusionPath, jobId);
  /// potentially setting permission
  return jobPath.getPath();
 }
 protected static RunningJob runFusionKeyCreationJob(JobConf inputConf, File tempStorage) {
  String jobName = "FusionKeyCreation";
  JobConf conf = new JobConf(getConf(), FusionJobClient.class);
  conf.setJobName(jobName);

  conf.setOutputKeyClass(inputConf.getOutputKeyClass());
  conf.setOutputValueClass(inputConf.getOutputKeyClass());

  conf.setMapperClass(FusionKeyCreationMap.class);
  conf.setReducerClass(FusionKeyCreationReduce.class);

  conf.setInputFormat(inputConf.getInputFormat());
  conf.setOutputFormat(TextOutputFormat.class);

  FileInputFormat.setInputPaths(conf, inputConf.getInputPaths());
  FileOutputFormat.setOutputPath(conf, (new File(tempStorage, jobName)).getPath());
  return JobClient.runJob(conf);
 }

 public static RunningJob runJob(JobConf conf, Defuser defuser) throws IOException {
  String jobId = UUID.randomUUID().toString();
  File tempDirectory = initTempStorage(jobId);
  RunningJob keyCreationJob = runFusionKeyCreationJob(conf);
  
  cleanUp(jobId);
  return null;
 }
 
 protected static void cleanUp(String jobId) {
  //TODO: clean up temp directories
 }
}
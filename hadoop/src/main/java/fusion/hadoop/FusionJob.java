package fusion.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fusion.hadoop.fusionkeycreation.FusionKeyCreationMap;
import fusion.hadoop.fusionkeycreation.FusionKeyCreationReduce;

public class FusionJob {

	protected JobConf m_conf;
	protected FusionJob(JobConf conf) {
		m_conf = new JobConf(conf);
	}
	
	public static FusionJob getInstance(JobConf conf) {
		return new FusionJob(conf);
	}
	
	protected final static String FUSION_PATH = "E:\\Java\\temp\\fusion_temp";
	protected static File initTempStorage(String jobId) {
		// TODO implement init temporary storage
		File fusionPath = new File(FUSION_PATH);
		File jobPath = new File(fusionPath, jobId);
		/// potentially setting permission
		return jobPath;
	}
	protected Job runFusionKeyCreationJob(File tempStorage) throws IOException, IllegalStateException, ClassNotFoundException {
		final String jobName = "FusionKeyCreation";
		Job job = Job.getInstance(m_conf);
		if (m_conf.getJar() != null) job.setJar(m_conf.getJar());
		job.setJobName(jobName);
		job.setOutputKeyClass(m_conf.getOutputKeyClass());
		job.setOutputValueClass(m_conf.getOutputKeyClass());

		job.setMapperClass(FusionKeyCreationMap.class);
		job.setReducerClass(FusionKeyCreationReduce.class);

//		job.setInputFormatClass(m_conf.getInputFormatClass());
//		job.setOutputFormatClass(m_conf.getOutputFormatClass()); // TODO consider TextOutputFormat.class
//		FileInputFormat.addInputPaths(job, m_conf.getConfiguration().get(FileInputFormat.INPUT_DIR));
		FileOutputFormat.setOutputPath(job, new Path(tempStorage.getPath()));
		return job;
	}

	
	public boolean waitForCompletion(boolean arg0) throws IOException, InterruptedException, ClassNotFoundException {
		String jobId = UUID.randomUUID().toString();
		File tempDirectory = initTempStorage(jobId);
		runFusionKeyCreationJob(tempDirectory);
		Job job = Job.getInstance(m_conf);
		return job.waitForCompletion(arg0);
	}
}
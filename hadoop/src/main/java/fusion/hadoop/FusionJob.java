package fusion.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
	
	//protected final static String FUSION_PATH = "E:\\Java\\temp\\fusion_temp";
	protected final static String FUSION_PATH = "/user/peter/fusion";
	protected Path initTempStorage(String jobId) throws IOException {
		Path tempJobPath = new Path(FUSION_PATH + Path.SEPARATOR_CHAR + jobId + Path.SEPARATOR_CHAR);
		FsPermission fsPerm = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
		FileSystem fs = FileSystem.newInstance(m_conf);
		fs.delete(tempJobPath, true);
		//fs.create(tempJobPath);
		//fs.setPermission(tempJobPath, fsPerm);
		fs.mkdirs(tempJobPath, fsPerm);
		//Path tempJobPath = new Path(FUSION_PATH);
		return tempJobPath;
	}
	
	protected Configuration createFusionConfiguration() {
		Configuration conf = new Configuration();
		conf.setClass(FusionConfigurationKeys.CONFIG_KEY_INPUT_MAPPER_CLASS, m_conf.getClass(MRJobConfig.MAP_CLASS_ATTR, null), Mapper.class);
		conf.setClass(FusionConfigurationKeys.CONFIG_KEY_INPUT_REDUCER, m_conf.getClass(MRJobConfig.REDUCE_CLASS_ATTR, null), Reducer.class);
		conf.set(FusionConfigurationKeys.CONFIG_KEY_RESULT_DIR, m_conf.get(FileOutputFormat.OUTDIR));
		return conf;
	}
	
	protected Job runFusionKeyCreationJob(Path tempPath) throws IOException, IllegalStateException, ClassNotFoundException {
		final Configuration fusionConf = createFusionConfiguration(); 
		final String jobName = "FusionKeyCreation";
		Job job = Job.getInstance(fusionConf);
		//if (m_conf.getJar() != null) job.setJar(m_conf.getJar());
		job.setJarByClass(FusionJob.class);
		job.setJobName(jobName);
		
		FileInputFormat.addInputPath(job, new Path(m_conf.get(FileInputFormat.INPUT_DIR)));
		FileOutputFormat.setOutputPath(job, new Path(tempPath, jobName));

		job.setOutputKeyClass(m_conf.getOutputKeyClass());
		job.setOutputValueClass(m_conf.getOutputKeyClass());

		job.setMapperClass(FusionKeyCreationMap.class);
		job.setReducerClass(FusionKeyCreationReduce.class);

		/// TODO: get from input job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

//		job.setInputFormatClass(m_conf.getInputFormatClass());
//		job.setOutputFormatClass(m_conf.getOutputFormatClass()); // TODO consider TextOutputFormat.class
		return job;
	}

	public boolean waitForCompletion(boolean arg0) throws IOException, InterruptedException, ClassNotFoundException {
		String jobId = UUID.randomUUID().toString();
		Path tempPath = initTempStorage(jobId);
		Job job = runFusionKeyCreationJob(tempPath);
		return job.waitForCompletion(arg0);
	}
}
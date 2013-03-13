package fusion.hadoop.fusionkeycreation;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ReflectionUtils;

import fusion.hadoop.FusionConfigurationKeys;

//public class FusionKeyCreationMap<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
public class FusionKeyCreationMap
// extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		extends Mapper<LongWritable, Text, Text, NullWritable> {

	public class KeyOnlyContext extends
			Mapper<LongWritable, Text, Text, IntWritable>.Context {

		protected Mapper<LongWritable, Text, Text, NullWritable>.Context m_context;

		protected KeyOnlyContext() {
			super();
		}

		public KeyOnlyContext(
				Mapper<LongWritable, Text, Text, NullWritable>.Context ctxt) {
			super();
			m_context = ctxt;
		}

		public InputSplit getInputSplit() {
			return m_context.getInputSplit();
		}

		// public KEYIN getCurrentKey() throws IOException, InterruptedException
		// {
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return m_context.getCurrentKey();
		}

		// public VALUEIN getCurrentValue() throws IOException,
		public Text getCurrentValue() throws IOException, InterruptedException {
			return m_context.getCurrentValue();
		}

		public OutputCommitter getOutputCommitter() {
			return m_context.getOutputCommitter();
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			return m_context.nextKeyValue();
		}

		public Counter getCounter(Enum<?> arg0) {
			return m_context.getCounter(arg0);
		}

		public Counter getCounter(String arg0, String arg1) {
			return m_context.getCounter(arg0, arg1);
		}

		public float getProgress() {
			return m_context.getProgress();
		}

		public String getStatus() {
			return m_context.getStatus();
		}

		public TaskAttemptID getTaskAttemptID() {
			return m_context.getTaskAttemptID();
		}

		public void setStatus(String arg0) {
			m_context.setStatus(arg0);
		}

		public Path[] getArchiveClassPaths() {
			return m_context.getArchiveClassPaths();
		}

		public String[] getArchiveTimestamps() {
			return m_context.getArchiveTimestamps();
		}

		public URI[] getCacheArchives() throws IOException {
			return m_context.getCacheArchives();
		}

		public URI[] getCacheFiles() throws IOException {
			return m_context.getCacheFiles();
		}

		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			return m_context.getCombinerClass();
		}

		public Configuration getConfiguration() {
			return m_context.getConfiguration();
		}

		public Credentials getCredentials() {
			return m_context.getCredentials();
		}

		public Path[] getFileClassPaths() {
			return m_context.getFileClassPaths();
		}

		public String[] getFileTimestamps() {
			return m_context.getFileTimestamps();
		}

		public RawComparator<?> getGroupingComparator() {
			return m_context.getGroupingComparator();
		}

		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {
			return m_context.getInputFormatClass();
		}

		public String getJar() {
			return m_context.getJar();
		}

		public JobID getJobID() {
			return m_context.getJobID();
		}

		public String getJobName() {
			return m_context.getJobName();
		}

		public boolean getJobSetupCleanupNeeded() {
			return m_context.getJobSetupCleanupNeeded();
		}

		@Deprecated
		public Path[] getLocalCacheArchives() throws IOException {
			return m_context.getLocalCacheArchives();
		}

		@Deprecated
		public Path[] getLocalCacheFiles() throws IOException {
			return m_context.getLocalCacheFiles();
		}

		public Class<?> getMapOutputKeyClass() {
			return m_context.getMapOutputKeyClass();
		}

		public Class<?> getMapOutputValueClass() {
			return m_context.getMapOutputValueClass();
		}

		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {
			return m_context.getMapperClass();
		}

		public int getMaxMapAttempts() {
			return m_context.getMaxMapAttempts();
		}

		public int getMaxReduceAttempts() {
			return m_context.getMaxReduceAttempts();
		}

		public int getNumReduceTasks() {
			return m_context.getNumReduceTasks();
		}

		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {
			return m_context.getOutputFormatClass();
		}

		public Class<?> getOutputKeyClass() {
			return m_context.getOutputKeyClass();
		}

		public Class<?> getOutputValueClass() {
			return m_context.getOutputValueClass();
		}

		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {
			return m_context.getPartitionerClass();
		}

		public boolean getProfileEnabled() {
			return m_context.getProfileEnabled();
		}

		public String getProfileParams() {
			return m_context.getProfileParams();
		}

		public IntegerRanges getProfileTaskRange(boolean arg0) {
			return m_context.getProfileTaskRange(arg0);
		}

		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {
			return m_context.getReducerClass();
		}

		public RawComparator<?> getSortComparator() {
			return m_context.getSortComparator();
		}

		@Deprecated
		public boolean getSymlink() {
			return m_context.getSymlink();
		}

		public boolean getTaskCleanupNeeded() {
			return m_context.getTaskCleanupNeeded();
		}

		public String getUser() {
			return m_context.getUser();
		}

		public Path getWorkingDirectory() throws IOException {
			return m_context.getWorkingDirectory();
		}

		public void progress() {
			m_context.progress();
		}

		public void write(Text key, IntWritable value) throws IOException,
				InterruptedException {
			m_context.write(key, NullWritable.get());
		}
	}

	// protected Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> m_inputMapper;
	protected Mapper<LongWritable, Text, Text, IntWritable> m_inputMapper;
	protected KeyOnlyContext m_koc;

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	/*
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		// TODO: invoke inputMapper's init
	}
	*/

	@Override
	public void run(Context context) throws IOException, InterruptedException {
		Class<Mapper<LongWritable, Text, Text, IntWritable>> mapperClass = 
				(Class<Mapper<LongWritable, Text, Text, IntWritable>>) 
				context.getConfiguration().getClass(FusionConfigurationKeys.CONFIG_KEY_INPUT_MAPPER_CLASS, String.class);
		m_inputMapper = (Mapper<LongWritable, Text, Text, IntWritable>) ReflectionUtils
				.newInstance(mapperClass, null);
		m_koc = new KeyOnlyContext(context);

		KeyOnlyContext koc = new KeyOnlyContext(context);
		m_inputMapper.run(koc);
	}
}

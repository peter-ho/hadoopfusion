package fusion.hadoop.fusionkeycreation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.ReflectionUtils;


public class FusionKeyCreationMap<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	protected Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> m_inputMapper;
	protected Context m_context;
	
	class KeyOnlyContext extends Context {
		protected Context m_ctxt;
		
		public KeyOnlyContext(Context ctxt) {
			m_ctxt = ctxt;
		}
		
		public InputSplit getInputSplit() {
			return m_context.getInputSplit();
		}

		public KEYIN getCurrentKey() throws IOException, InterruptedException {
			return m_context.getCurrentKey();
		}

		public VALUEIN getCurrentValue() throws IOException,
				InterruptedException {
			return m_context.getCurrentValue();
		}

		public OutputCommitter getOutputCommitter() {
			return m_context.getOutputCommitter();
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			return m_context.nextKeyValue();
		}

		public void write(KEYOUT arg0, VALUEOUT arg1) throws IOException,
				InterruptedException {
			m_context.write(arg0, null);
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
		
	}

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		try {
			m_inputMapper = (Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>)
					ReflectionUtils.newInstance(context.getMapperClass(), null);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		KeyOnlyContext koc = new KeyOnlyContext(context);
		m_inputMapper.run(koc);
	}
}

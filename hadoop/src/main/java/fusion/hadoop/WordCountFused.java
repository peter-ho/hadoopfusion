package fusion.hadoop;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import fusion.hadoop.defuseMissedKeys.DefuseMissedKeys;
import fusion.hadoop.defuseMissedKeys.DefuseMissedKeysWithoutMapFile;
import fusion.hadoop.fusionexecution.FusionExecution;
import fusion.hadoop.fusionexecution.FusionExecution2;
import fusion.hadoop.fusionexecution.FusionExecution3;
import fusion.hadoop.fusionkeycreation.FusionKeyCreateToSeqFile;
import fusion.hadoop.fusionkeycreation.FusionKeyCreation;
import fusion.hadoop.fusionkeycreation.FusionKeyCreation2;
import fusion.hadoop.fusionkeycreation.FusionKeyCreation3;
import fusion.hadoop.fusionkeycreation.MapFileParser;
import fusion.hadoop.missedKeySearch.MissedKeySearch;
import fusion.hadoop.missedKeySearch.MissedKeySearch3;


// https://ccp.cloudera.com/display/CDH4DOC/Using+the+CDH4+Maven+Repository
public class WordCountFused
{
	public static class WordCountMapper
	extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

		public static String charsToSkip = " \n\r\t\f~`!@#$%^&*()_-+={}[]|\\\":;'<>,.?/";
		public static String[] patternsToSkip = {
			"&amp;",
			"&lt;",
			"&quot;",
			"&gt;",
			"nbsp;",
			"dash;",
		};

		
		public static ArrayList<String> map(Text value)
				throws IOException, InterruptedException {
			String strValue = value.toString();
			
			for (String pattern : patternsToSkip) {
				strValue = strValue.replaceAll(pattern, " ").toLowerCase();
		    }
			
			ArrayList<String> keys = new ArrayList<String>();
			StringTokenizer tokenizer = new StringTokenizer(strValue, charsToSkip, false);
			while (tokenizer.hasMoreTokens()) {
				keys.add(tokenizer.nextToken());
			}
			return keys;			
		}
	}

	public static class WordCountReducer
	extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
						throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main1( String[] args ) throws IOException, InterruptedException, ClassNotFoundException
	{
		if (args.length != 2) 
		{
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}

		JobConf conf = new JobConf();
		conf.setJarByClass(WordCount.class);
		conf.setJobName("Word Count with Fusion");

		conf.set(FileInputFormat.INPUT_DIR, args[0]);
		conf.set(FileOutputFormat.OUTDIR, args[1]);
		conf.setClass(MRJobConfig.MAP_CLASS_ATTR, WordCountMapper.class, Mapper.class);
		conf.setClass(MRJobConfig.REDUCE_CLASS_ATTR, WordCountReducer.class, Reducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FusionJob job = FusionJob.getInstance(conf);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main( String[] args ) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		System.out.println("\n*** WordCountFusion start...");
		if (args.length != 2) 
		{
			System.err.println("Usage:: WordCount <input path> <output path>");
			System.exit(-1);
		}
		long msStart = System.currentTimeMillis();
		final String fusionKeyMapPath =  "/user/peter/fusion/FusionKeyMap";
		final String missingKeySearchResult = "/user/peter/fusion/MissingKeySearchResult";
		final String missingKeyDefuseResult = "/user/peter/fusion/MissingKeyDefuseResult";
		final String executionResultPath = args[1];
		final String inputPath = args[0];
		//final String fusionKeyMapPath =  "/user/peter/fusion/FusionKeyMap";
		
		long msTemp = System.currentTimeMillis();
		int status = 0;
		status = FusionKeyCreation3.main(args[0], fusionKeyMapPath);
		System.out.println("*** Job elapsed: " + (System.currentTimeMillis() - msTemp) + "ms\n"); msTemp = System.currentTimeMillis();
		//if (status == 0) status = FusionExecution.main(inputPath, fusionKeyMapPath, executionResultPath);
		if (status == 0) status = FusionExecution3.main(fusionKeyMapPath, executionResultPath);
		System.out.println("*** Job elapsed: " + (System.currentTimeMillis() - msTemp) + "ms\n"); msTemp = System.currentTimeMillis();
		//if (status == 0) status = MissedKeySearch.main(executionResultPath + "/result-r-*", fusionKeyMapPath + "/fusionkey-r-*", missingKeySearchResult);
		//if (status == 0) status = MissedKeySearch.main(executionResultPath + "/result-r-*", MapFileParser.PATH + "/part-r-*/data", missingKeySearchResult);
		//if (status == 0) status = MissedKeySearch.main(executionResultPath + "/result-r-*", MapFileParser.PATH + "/fusionkey-r-*", missingKeySearchResult);
		if (status == 0) status = MissedKeySearch3.main(executionResultPath + "/result-r-*", MapFileParser.PATH + "/fusionkeyvalue-r-*", missingKeySearchResult);
		System.out.println("*** Job elapsed: " + (System.currentTimeMillis() - msTemp) + "ms\n"); msTemp = System.currentTimeMillis();
		//if (status == 0) status = DefuseMissedKeys.main(executionResultPath + "/result-r-*", executionResultPath, missingKeySearchResult, missingKeyDefuseResult);
		if (status == 0) status = DefuseMissedKeysWithoutMapFile.main(executionResultPath + "/result-r-*", executionResultPath, missingKeySearchResult, missingKeyDefuseResult);
		System.out.println("*** Job elapsed: " + (System.currentTimeMillis() - msTemp) + "ms\n"); msTemp = System.currentTimeMillis();
		long msEnd = System.currentTimeMillis();
		System.out.println("\n*** Total elapsed: " + (msEnd - msStart) + "ms");
		System.exit(status);
	}

}

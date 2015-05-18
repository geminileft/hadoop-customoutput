package dev.geminileft.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dev.geminileft.mappers.TwoPartMapper;
import dev.geminileft.reducers.TwoPartABCacheReducer;

public class TwoPartABCacheTest extends Configured implements Tool {
	
	private static final int INPUT_PATH_ARG_INDEX = 0;
	private static final int OUTPUT_PATH_ARG_INDEX = 1;
	private static final int CACHE_FILE_ARG_INDEX = 2;
	
	 public static void main(String[] args) throws Exception {
	     int exitCode = ToolRunner.run(new Configuration(), new TwoPartABCacheTest(), args);
	     System.exit(exitCode);
	 }
	 
	 public int run(String[] args) throws Exception {
	   Configuration conf = this.getConf();
	   Job job = Job.getInstance(conf, "TwoPartABCacheTest");
	   job.addCacheFile(
			   new Path(String.format("%s"
				, args[CACHE_FILE_ARG_INDEX])).toUri());
	   job.setInputFormatClass(TextInputFormat.class);
	   job.setOutputFormatClass(TextOutputFormat.class);
	   job.setJarByClass(TwoPartABCacheTest.class);
	   job.setMapperClass(TwoPartMapper.class);
	   job.setReducerClass(TwoPartABCacheReducer.class);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(FloatWritable.class);
	   FileInputFormat.addInputPath(job, new Path(args[INPUT_PATH_ARG_INDEX]));
	   FileOutputFormat.setOutputPath(job, new Path(args[OUTPUT_PATH_ARG_INDEX]));
	   return job.waitForCompletion(true) ? 0 : 1;
	 }

}

package dev.geminileft.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import dev.geminileft.mappers.XmlMapper;
import dev.geminileft.outputformat.TextRecordOutputFormat;
import dev.geminileft.reducers.TextReducer;

public class XmlFlatApp  extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new XmlFlatApp(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "XmlFlatApp");
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextRecordOutputFormat.class);
		job.setJarByClass(XmlFlatApp.class);
		job.setMapperClass(XmlMapper.class);
		job.setReducerClass(TextReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}

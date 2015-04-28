package dev.geminileft;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextRecordOutputFormat<K> extends FileOutputFormat<K, Text> {
	
	public static String DELIMITER_CONFIG = "textrecordoutputformat.delimiter";
	
	public static class TextRecordWriter<K> extends RecordWriter<K, Text> {
		
		private static String ENCODING = "UTF-8";
	    private DataOutputStream mOutputStream;
	    private byte[] mDelimiter;

	    public TextRecordWriter(DataOutputStream stream, String delimiter) throws UnsupportedEncodingException {
	        mOutputStream = stream;
	        mDelimiter = delimiter.getBytes(ENCODING);
	    }

	    @Override
	    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	        //close our file
	        mOutputStream.close();
	    }

	    @Override
	    public void write(K key, Text value) throws IOException, InterruptedException {
	        mOutputStream.write(value.toString().getBytes(ENCODING));
	        mOutputStream.write(mDelimiter);
	    }
	}
	
	@Override
	public RecordWriter<K, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		String delimiter = config.get(DELIMITER_CONFIG, "\n");
		Path outputPath = getDefaultWorkFile(context, "");
		FileSystem fs = outputPath.getFileSystem(config);
		FSDataOutputStream outputStream = fs.create(outputPath, false);
		return new TextRecordWriter<K>(outputStream, delimiter);
	}
}

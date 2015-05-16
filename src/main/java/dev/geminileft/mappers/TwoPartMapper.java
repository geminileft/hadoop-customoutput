package dev.geminileft.mappers;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoPartMapper extends Mapper<Object, Text, Text, FloatWritable> {
	public static final String DELIMITER = "/";
	public static final int PARTA_INDEX = 0;
	public static final int PARTB_INDEX = 1;
	private static final int FLOAT_INDEX = 2;
	private static final String SPLIT_STRING = ",";
	
	private static Text mKey = new Text();
	private static FloatWritable mValue = new FloatWritable();
	
	public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
		String[] split = value.toString().split(SPLIT_STRING);
		mKey.set(String.format("%s%s%s", split[PARTA_INDEX], DELIMITER, split[PARTB_INDEX]));
		mValue.set(Float.parseFloat(split[FLOAT_INDEX]));
		context.write(mKey, mValue);
	}
}

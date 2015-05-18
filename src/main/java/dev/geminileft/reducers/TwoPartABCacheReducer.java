package dev.geminileft.reducers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import dev.geminileft.mappers.TwoPartMapper;

public class TwoPartABCacheReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	
	private static final int CACHE_FILE_INDEX = 0;
	private static final String DELIMITER = ",";
	private static final int KEY_INDEX = 0;
	private static final int VALUE_INDEX = 1;	
	
	private static Text mKey = new Text();
	private static FloatWritable mValue = new FloatWritable();
	
	private HashMap<String, Float> mLookup = new HashMap<String, Float>();
	
	@Override
    protected void setup(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();
		if (cacheFiles != null
	            && cacheFiles.length > 0) {
	        File f = new File(cacheFiles[CACHE_FILE_INDEX].getPath());
	        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
	            String line;
	            while ((line = br.readLine()) != null) {
	               String[] split = line.split(DELIMITER);
	               mLookup.put(split[KEY_INDEX], Float.parseFloat(split[VALUE_INDEX]));
	            }
	        }
		}
	}
	
	public void reduce(Text key, Iterable<FloatWritable> values,
            Context context
            ) throws IOException, InterruptedException {
		String[] split = key.toString().split(TwoPartMapper.DELIMITER);
		String partA = split[TwoPartMapper.PARTA_INDEX];
		String partB = split[TwoPartMapper.PARTB_INDEX];
		
		float v1 = 0;
		for (FloatWritable f : values) {
			v1 += f.get();
		}
		
		mKey.set(String.format("%s\t%s", partA, partB));
		Float v2a = mLookup.get(partA);
		Float v2b = mLookup.get(partB);
		mValue.set(v1 / (float)Math.sqrt(v2a.floatValue() * v2b.floatValue()));
		context.write(mKey, mValue);		
	}
}

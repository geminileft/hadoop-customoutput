import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestInputSplit extends Configured implements Tool {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word;
    public final static String OUTPUT_TEXT_KEY = "output.text";

    @Override
    protected void setup(Context context) {
    	Configuration config = context.getConfiguration();
    	String temp = config.get(OUTPUT_TEXT_KEY);
    	if (temp == null) {
    		temp = "total";
    	}
    	word = new Text(temp);
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
        context.write(word, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new Configuration(), new TestInputSplit(), args);
      System.exit(exitCode);
  }
  
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    String temp = conf.get("input.delim");
	if (temp != null) {
		conf.set("textinputformat.record.delimiter", temp);
	}
    Job job = Job.getInstance(conf, "test input split");
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(MyTextOutputFormat.class);
    job.setJarByClass(TestInputSplit.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}

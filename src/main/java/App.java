import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class App extends Configured implements Tool {

  public static class XmlMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text mKey = new Text();
    public final static String OUTPUT_TEXT_KEY = "output.text";
    public final static String FILTER_KEY = "filter.keys";
    public final static String DEFAULT_KEY_VALUE = "total";
    public final static int UPC_KEY = 0;
    public final static int ID_KEY = 1;
    public final static String FIELD_SEPARATOR = "~";
    
    private TreeMap<String, TreeSet<String>> mKeys = new TreeMap<String, TreeSet<String>>(String.CASE_INSENSITIVE_ORDER);
    
    @Override
    protected void setup(Context context) {
    	Configuration config = context.getConfiguration();
    	String temp = config.get(OUTPUT_TEXT_KEY);
    	if (temp == null) {
    		temp = DEFAULT_KEY_VALUE;
    	}
    	
    	Gson gson = new Gson();
    	temp = config.get(FILTER_KEY);
		Type t = new TypeToken<TreeMap<String, HashSet<String>>>(){}.getType();
		TreeMap<String, HashSet<String>> tempMap = gson.fromJson(temp, t);
		for (Map.Entry<String, HashSet<String>> entry : tempMap.entrySet()) {
			TreeSet<String> tempSet = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
			tempSet.addAll(entry.getValue());
			mKeys.put(entry.getKey(), tempSet);
		}
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			SAXParser parser = factory.newSAXParser();
			XMLReader xmlreader = parser.getXMLReader();
			SaxHandler handler = prepareSaxHandler();
			xmlreader.setContentHandler(handler);
			String temp = value.toString();
			int index = temp.indexOf("_");
			mKey.set(temp.substring(0, index));
			xmlreader.parse(
					new InputSource(
							new StringReader(temp.substring(index + 1))));
			for (String s : handler.getValues()) {
				String[] split = s.split(FIELD_SEPARATOR);
				String upc = split[UPC_KEY];
				String id = split[ID_KEY];
				TreeSet<String> ids = mKeys.get(upc);
				if (ids != null) {
					if (ids.contains(id)) {
						mKey.set(upc);
						context.write(mKey, new Text(id));
					}
				}
				mKey.set(upc);
				//context.write(mKey, new Text(s));
			}
		} catch (Exception e) {
			context.write(new Text("error"), new Text(e.getMessage()));
		}
    }
    
	public static SaxHandler prepareSaxHandler() {
		HashMap<String, SaxHandler.AttributeCapture> nodeAttributes = new HashMap<String, SaxHandler.AttributeCapture>();
		SaxHandler.AttributeCapture capture;
		HashMap<String, Integer> attributeOutput;
		
		attributeOutput = new HashMap<String, Integer>();
		attributeOutput.put("upc", UPC_KEY);
		capture = new SaxHandler.AttributeCapture(attributeOutput);
		nodeAttributes.put("/items/i", capture);
		
		attributeOutput = new HashMap<String, Integer>();
		attributeOutput.put("id", ID_KEY);
		capture = new SaxHandler.AttributeCapture(attributeOutput, true);
		nodeAttributes.put("/items/i/item", capture);
		
		SaxHandler handler = new SaxHandler(nodeAttributes, FIELD_SEPARATOR);
		return handler;
	}

  }

  public static class TextReducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	StringBuilder sb = new StringBuilder();
    	String delim = "";
      for (Text val : values) {
    	  sb.append(String.format("%s%s", delim, val.toString()));
    	  delim = ",";
      }
      context.write(key, new Text(sb.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new Configuration(), new App(), args);
      System.exit(exitCode);
  }
  
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    String temp = conf.get("input.delim");
	if (temp != null) {
		conf.set("textinputformat.record.delimiter", temp);
	}
    Job job = Job.getInstance(conf, "xml test");
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(MyTextOutputFormat.class);
    job.setJarByClass(App.class);
    job.setMapperClass(XmlMapper.class);
    job.setCombinerClass(TextReducer.class);
    job.setReducerClass(TextReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}

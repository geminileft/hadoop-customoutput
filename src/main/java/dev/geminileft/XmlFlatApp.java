package dev.geminileft;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

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

public class XmlFlatApp  extends Configured implements Tool {

	  public static class XmlMapper
      extends Mapper<Object, Text, Text, Text>{

   public final static int UPC_KEY = 0;
   public final static int ID_KEY = 1;
   public final static String FIELD_SEPARATOR = "~";
   public final static Text mKey = new Text("");
   
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
			String recordKey = temp.substring(0, index); 
			xmlreader.parse(
					new InputSource(
							new StringReader(temp.substring(index + 1))));
			for (String s : handler.getValues()) {
				String mapValue = String.format("%s%s%s", recordKey, FIELD_SEPARATOR, s);
				context.write(mKey, new Text(mapValue));
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
     for (Text val : values) {
         context.write(key, val);
     }
   }
 }

 public static void main(String[] args) throws Exception {
     int exitCode = ToolRunner.run(new Configuration(), new XmlFlatApp(), args);
     System.exit(exitCode);
 }
 
 public int run(String[] args) throws Exception {
   Configuration conf = this.getConf();
   Job job = Job.getInstance(conf, "xml test");
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextRecordOutputFormat.class);
   job.setJarByClass(XmlFlatApp.class);
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

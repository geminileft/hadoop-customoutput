package dev.geminileft;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SaxHandler extends DefaultHandler {

	public static class AttributeCapture {
		
		private final boolean mHasOutput;
		private final HashMap<String, Integer> mAttributeOutput;
		
		public AttributeCapture(HashMap<String, Integer> attributeOutput) {
			this(attributeOutput, false);
		}
		
		public AttributeCapture(HashMap<String, Integer> attributeOutput, boolean hasOutput) {
			mHasOutput = hasOutput;
			mAttributeOutput = attributeOutput;
		}
		
		public Set<String> getAttributes() {
			return mAttributeOutput.keySet();
		}
		
		public boolean hasOutput() {
			return mHasOutput;
		}
		
		public Integer getOrdinal(String key) {
			return mAttributeOutput.get(key);
		}
	}
	
	private Stack<String> mNodePosition = new Stack<String>();
	private final HashMap<String, AttributeCapture> mAttributes;
	private HashMap<String, HashMap<String, String>> mNodeValues = new HashMap<String, HashMap<String, String>>(); 
	private final String mSeparator;
	private LinkedList<String> mValues = new LinkedList<String>();
	
	public SaxHandler(HashMap<String, AttributeCapture> nodeAttributes, String separator) {
		mSeparator = separator;
		mAttributes = nodeAttributes;
		for (Entry<String, AttributeCapture> nodeAttribute : nodeAttributes.entrySet()) {
			HashMap<String, String> attributeData = new HashMap<String, String>();
			for (String attribute : nodeAttribute.getValue().getAttributes()) {
				attributeData.put(attribute, null);
				mNodeValues.put(nodeAttribute.getKey(), attributeData);
			}
		}
	}
	
    public void startElement(String uri, String localName,
        String qName, Attributes attributes)
    throws SAXException {
    	mNodePosition.push(qName);
    	String xpath = buildXPath(mNodePosition);
    	HashMap<String, String> nodeAttributes = mNodeValues.get(xpath);
    	if (nodeAttributes != null) {
    		for (String attrib : nodeAttributes.keySet()) {
    			nodeAttributes.put(attrib, attributes.getValue(attrib));
    		}
    	}
        //System.out.println("start element    : " + qName);
    }

    public void endElement(String uri, String localName, String qName)
    throws SAXException {
    	String xpath = buildXPath(mNodePosition);
    	mNodePosition.pop();
    	HashMap<String, String> nodeAttributes = mNodeValues.get(xpath);
    	if (nodeAttributes != null) {
    		AttributeCapture capture = mAttributes.get(xpath);
    		if (capture.hasOutput()) {
    	    	HashMap<Integer, String> output = new HashMap<Integer, String>();
        		for (String attrib : nodeAttributes.keySet()) {
        			String attribValue = nodeAttributes.get(attrib);
        			output.put(capture.getOrdinal(attrib), attribValue);
        			nodeAttributes.put(attrib, null);
        		}
    			Stack<String> buildStack = new Stack<String>();
	    		for (int i = 0;i < mNodePosition.size();++i) {
	    			buildStack.push(mNodePosition.get(i));
	    	    	xpath = buildXPath(buildStack);
	    	    	HashMap<String, String> parentAttributes = mNodeValues.get(xpath);
	    	    	if (parentAttributes != null) {
	    	    		AttributeCapture parentCapture = mAttributes.get(xpath);
	    	    		for (String parentAttrib : parentAttributes.keySet()) {
	    	    			String parentValue = parentAttributes.get(parentAttrib);
	    	    			output.put(parentCapture.getOrdinal(parentAttrib), parentValue);
	    	    		}
	    	    	}
	    		}
	    		Integer[] outputKeys = output.keySet().toArray(new Integer[output.size()]);
	    		Arrays.sort(outputKeys);
	    		StringBuilder sb = new StringBuilder();
	    		String separator = "";
	    		for (Integer outputKey : outputKeys) {
	    			sb.append(separator);
	    			sb.append(output.get(outputKey));
	    			separator = mSeparator;
	    		}
	    		mValues.add(sb.toString());
    		}
    	}
    }
    
    private static String buildXPath(Stack<String> pathStack) {
    	StringBuilder xpathResult = new StringBuilder();
    	for (String s : pathStack) {
    		xpathResult.append(String.format("/%s", s));
    	}
    	return xpathResult.toString();
    }
    
    public LinkedList<String> getValues() {
    	return mValues;
    }
}

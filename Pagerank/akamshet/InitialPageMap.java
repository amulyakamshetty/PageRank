//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.File;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


//map class to work on the outlinks
public class InitialPageMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		String get_title[] = new String[2]; //we will initialize string array of size 2
		
		String line = lineText.toString(); //we are getting the data betwen the tags <title> and </title> 
		
		if (line.contains("<title>") && line.contains("</title>")) {
			int data_start = lineText.find("<title>");  //we will store the start index of the data 
			int data_end = lineText.find("</title>", data_start);  //we will store the end index of data, start reading from start of data
			data_start = data_start + 7;
			int x = data_end - data_start; //we will store the number of bytes of data
			if (data_start != data_end && data_end > data_start) {
				get_title[0] = Text.decode(lineText.getBytes(), data_start, x); //if there is data present between the indices
			} else
				get_title[0] = " ";
		} else
			get_title[0] = " ";

		// we will get the data  present between the tags <text> and </text>
		try{
			boolean check = true;
		String outlinks = getText(lineText); 
		if (outlinks != null)
			context.write(new Text(get_title[0]),new Text(outlinks)); //write out the output
		
	}
	catch (Exception e)
		{
		e.printStackTrace();
		}
		}
	
	//now the outlinks have to be pulled out
	public String getText(Text line) {
		Pattern linkPat = Pattern.compile("\\[\\[.*?]\\]");
		String url = "";
		String output = "";
		String line_String = line.toString();
		Matcher m = linkPat.matcher(line_String);
		
		//pull out the outlinks
		while (m.find()) { 
			url = m.group().replace("[[", "").replace("]]", ""); // we will delete brackets if present and also nested text if present
			if (!url.isEmpty()) 
			{
				
				if(output.equals("")) //suppose there is just a single outlink
				output=output+url;
				else
					output=output+";"+url; //we are inserting a delimiter btwn outlinks	
				} 
			
		}
		return output; //now return the final output 	 
		}
		}
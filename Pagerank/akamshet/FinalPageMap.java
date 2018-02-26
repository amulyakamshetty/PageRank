//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//map class to calculate outlinks 
public class FinalPageMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		try {
			context.getInputSplit();
			String line = lineText.toString(); //we convert line to string
			String split_t[] = {""};      //initialize an array of type string    
			
			split_t = line.split("\t");           //we split given line on tab
			String page_title = split_t[0];      //we get the title of the page
			String page_rank_value = split_t[2]; //we get the pagerank of the page
			if(split_t[1].equals("")){              //split will give us the outlinks and suppose outlinks are not present
				context.write(new Text(page_title), new Text("flag"+"\t"+""));         // we will insert a flag together with the title
				context.write(new Text("temp"), new Text( page_rank_value+"\t"+"temp")); //we will get value of pagerank
				return;
			}
			else{
				if(split_t[1].contains(";")){   //suppose there are outlinks in the title
					String outlinks[]=split_t[1].split(";");       //we will acquire the outlinks which were split on the delimiter
					context.write(new Text(page_title), new Text("flag"+"\t"+split_t[1]) );     
					for(String word:outlinks)
						context.write(new Text(word), new Text(split_t[2]+"\t"+outlinks.length));   //we will write outlink pagerank and the outlink length
					}
				else{ 
					context.write(new Text(split_t[1]), new Text(split_t[2]+"\t"+1));  //suppose just one outlink is present, outlink and the respective value of pagerank and outlink number is one
				context.write(new Text(page_title), new Text("flag"+"\t"+split_t[1]) );}    //we will write title of page and flag and the outlinks
				
			}
				} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
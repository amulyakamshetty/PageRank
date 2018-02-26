//Amulya Kamshetty
//800962032
//akamshet@uncc.edu

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//reducer to calculate page rank value
public class FinalPageReduce extends Reducer<Text,Text, Text, Text> {

public void reduce(Text title, Iterable<Text> countvalue, Context context)throws IOException, InterruptedException {
	double sum=0;int temp=0;String outlinks="";
	if(title.equals("")){  //suppose no title is present 
		return;				
	}
	for(Text x:countvalue){               
		String list[]=x.toString().split("\t");  //we will split values which we got from map
		if(list[0].equals("flag"))               //suppose we have a flag 
			{ 
			temp=1;                             //we will set temp to 1
			if(list.length==2)                   //suppose we have the length of split list equal to 2
			outlinks=list[1];                    //we will set outlinks as one
			}
		else
		{
			if(!(list.length==2))                 //suppose we encounter a dangling node then we continue
				continue;
			else{
				if(list[1].equals("temp")&&title.toString().equals("temp"))
				{
					System.out.println("no outlinks");break;
				}
				else
			{sum=sum+(Double.parseDouble(list[0])/Double.parseDouble(list[1]));} }  //the total is calculated by divinding pagerank among all the outlinks from that node  
		}	
	}
	if(temp==1)        //suppose a flag is present
	{
		double result=0.85*sum+0.15;  //formula given to calculate pagerank
		
		context.write(new Text(title),new Text(outlinks+"\t"+result));    //we calcualte pagerank for each page
	}
	
	

}
}
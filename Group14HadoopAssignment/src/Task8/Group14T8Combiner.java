package Task8;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Task Done By : Md Zulfiqah (1602575) **/
public class Group14T8Combiner extends Reducer<Text, Text, Text, Text> {
	
	SentiWordNet test;
	
	@Override
	protected void setup(Context context){
		test = new SentiWordNet();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
		String firstSentiment = values.iterator().next().toString();
		String sentimentValue = "";
		
		String[] words = key.toString().split("\\s+"); 
		
		double totalScore = 0;
		
		for(String word : words) {
		    word = word.replaceAll("([^a-zA-Z\\s])", "");
		    if (test.extract(word) == null)
		        continue;
		    totalScore += test.extract(word);
		}
		
		if(totalScore > 0) {
			sentimentValue = "positive";
		}
		else if(totalScore == 0) {
			sentimentValue = "neutral";
		}
		else if(totalScore < 0){
			sentimentValue = "negative";
		}
		
		if(firstSentiment.equals(sentimentValue)){
			context.write(new Text("Result"), new Text("1"));	
		}
		else{
			context.write(new Text("Result"), new Text("0"));	
		}	
	}
}

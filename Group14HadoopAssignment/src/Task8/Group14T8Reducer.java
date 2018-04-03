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

public class Group14T8Reducer extends Reducer<Text, Text, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
		System.out.println("Hel");
		int totalCount = 0;
		int similarityCount = 0;
		
		for(Text value : values) {
			String aKey = key.toString();
			String aValue = value.toString();
			String sentimentValue = "";
			
			SentiWordNet test = new SentiWordNet();
			String[] words = aValue.split("\\s+"); 
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
			
			if(sentimentValue.equals(aKey)) {
				similarityCount ++;
			}
			
			totalCount++;
		}
		
		int percentage = similarityCount / totalCount;
		System.out.println("Hello" + percentage);
		
		context.write(new Text("Percentage"), new IntWritable(percentage));		
	}
}

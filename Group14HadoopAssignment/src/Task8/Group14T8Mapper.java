package Task8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Group14T8Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text tweetText = new Text();
	Text tweetSentimentGold = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String tweetTextStr;
			String tweetSentimentGoldStr = "";
			if(parts.length == 27 && (parts[17].contains("negative") || parts[17].contains("neutral") || parts[17].contains("positive")) && parts[21] != null) {
				tweetSentimentGoldStr = parts[17].trim();
				tweetTextStr = parts[21].trim();
				
				System.out.println(tweetSentimentGoldStr);
				System.out.println(tweetTextStr);
				
								
				if(tweetSentimentGoldStr!=null && !tweetSentimentGoldStr.isEmpty() && tweetTextStr!=null && !tweetTextStr.isEmpty()) {					
					tweetSentimentGold.set(tweetSentimentGoldStr);
					tweetText.set(tweetTextStr);
					
					context.write(tweetSentimentGold, tweetText);
				}
			}
			else {
				//continue;
			}
	}
	
	
}

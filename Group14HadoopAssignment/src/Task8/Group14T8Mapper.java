package Task8;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Md Zulfiqah (1602575) **/
public class Group14T8Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text tweetText = new Text();
	Text tweetSentimentGold = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String tweetTextStr;
			String tweetSentimentGoldStr = "";
			if(parts.length >= 22 && !parts[17].isEmpty() && parts[21] != null) {
				tweetSentimentGoldStr = parts[17].trim();
				tweetTextStr = parts[21].trim();
												
				if(tweetSentimentGoldStr!=null && !tweetSentimentGoldStr.isEmpty() && tweetTextStr!=null && !tweetTextStr.isEmpty()) {					
					
					tweetText.set(tweetTextStr);
					tweetSentimentGold.set(tweetSentimentGoldStr);
					
					context.write(tweetText, tweetSentimentGold);
				}
			}
	}
}

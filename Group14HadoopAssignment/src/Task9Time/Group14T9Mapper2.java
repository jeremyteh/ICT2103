package Task9Time;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Teo Hwee Boon (1602086)**/
public class Group14T9Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	Text ipAddress = new Text();
	Text dateOfTweet = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String ipAddressStr;
			String dateOfTweetStr;
			
			if(key.get() == 0){
				
			}
			else if(parts.length >= 24 && parts[13] != null && parts[23] != null) {
				ipAddressStr = parts[13].trim();
				dateOfTweetStr = parts[23].trim();
				
				if(dateOfTweetStr!=null && !dateOfTweetStr.isEmpty() ) {					
					ipAddress.set(ipAddressStr);
					dateOfTweet.set(dateOfTweetStr);
					context.write(ipAddress, dateOfTweet);
				}
			}
	}
}

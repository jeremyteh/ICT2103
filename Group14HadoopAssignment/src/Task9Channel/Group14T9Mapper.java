package Task9Channel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Teh Yong Sheng, Jeremy (1602514)**/
public class Group14T9Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text channel = new Text();
		
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String channelStr;
			
			if(key.get() == 0){
				
			}
			else if(parts.length >= 8 && parts[7] != null) {
				channelStr = parts[7].trim();
				
				if(channelStr!=null && !channelStr.isEmpty() ) {					
					
					channel.set(channelStr);
					context.write(channel, new IntWritable(1));
				}
			}
	}
}

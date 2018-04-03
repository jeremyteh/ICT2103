package Task9One;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Group14T9Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	HashMap<String, Integer> ChannelCountMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
		for(IntWritable value : values) {
			
			String aKey = key.toString();
			int aValue = value.get();
			
			if(!ChannelCountMap.containsKey(aKey)) {
				
				ChannelCountMap.put(aKey, aValue);
			}
			else {
				
				int currentCount = ChannelCountMap.get(aKey);
				ChannelCountMap.put(aKey, currentCount+1);
			}
			
		}
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		HashMap<String, Integer> sortedChannelCountMap = sortHashMapByValues(ChannelCountMap);
		
		String channel;
		int numberofTweets;
		
		for(Map.Entry<String, Integer> channelCountCountEntry : sortedChannelCountMap.entrySet()) {
							
			channel = channelCountCountEntry.getKey();
			numberofTweets = channelCountCountEntry.getValue();
			
			context.write(new Text(channel), new IntWritable(numberofTweets));			
		}	
	}
	
	// method to sort the HashMap
	public LinkedHashMap<String, Integer> sortHashMapByValues(HashMap<String, Integer> passedMap) {
	    
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(passedMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
			
			public int compare(Map.Entry<String, Integer> countryCount1, Map.Entry<String, Integer> countryCount2) {
				return countryCount2.getValue().compareTo(countryCount1.getValue());
			}
		});
		
		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
		
		for(Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		
		return sortedMap;
	}
}

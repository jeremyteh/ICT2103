package Task4;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Task Done By : Teh Yong Sheng, Jeremy (1602514) **/
public class Group14T4Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	HashMap<String, Integer> AirlineCountMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
		for(IntWritable value : values) {
			
			String aKey = key.toString();
			int aValue = value.get();
			
			if(!AirlineCountMap.containsKey(aKey)) {
				
				AirlineCountMap.put(aKey, aValue);
			}
			else {
				
				int currentCount = AirlineCountMap.get(aKey);
				AirlineCountMap.put(aKey, currentCount+1);
			}
			
		}		
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		int counter = 0;
		
		HashMap<String, Integer> sortedAirlineCountMap = sortHashMapByValues(AirlineCountMap);
		
		String country;
		int numberOfPosTweets;
		
		for(Map.Entry<String, Integer> airlineCountEntry : sortedAirlineCountMap.entrySet()) {
			
			if(counter < 3) {
				
				country = airlineCountEntry.getKey();
				numberOfPosTweets = airlineCountEntry.getValue();
				
				context.write(new Text(country), new IntWritable(numberOfPosTweets));
				
				counter++;
			}
			else{ 
				counter = 0;
				break;
			}			
		}		
	}
	
	// method to sort the HashMap
	public LinkedHashMap<String, Integer> sortHashMapByValues(HashMap<String, Integer> passedMap) {
	    
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(passedMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
			
			public int compare(Map.Entry<String, Integer> reasonCount1, Map.Entry<String, Integer> reasonCount2) {
				return reasonCount2.getValue().compareTo(reasonCount1.getValue());
			}
		});
		
		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
		
		for(Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		
		return sortedMap;
	}
}

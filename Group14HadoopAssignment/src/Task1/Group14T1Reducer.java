package Task1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

/** Task Done By : Aw Yee Cheong (1602328) **/
public class Group14T1Reducer extends Reducer<Text, Text, Text, Text> {
	
	HashMap<String, HashMap> AirlineNegReasonMap = new HashMap<String, HashMap>();
	HashMap<String, Integer> NegReasonCountMap;
	int TotalReasonCount = 0;
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
		for(Text value : values) {
			
			String aKey = key.toString();
			String aValue = value.toString();
			TotalReasonCount += 1;
			
			if(!AirlineNegReasonMap.containsKey(aKey)) {
				
				NegReasonCountMap = new HashMap<String, Integer>();
				
				NegReasonCountMap.put(aValue, 1);
				//System.out.println(key.toString() +" : " +  value.toString());
				AirlineNegReasonMap.put(aKey, NegReasonCountMap);
			}
			else if(AirlineNegReasonMap.containsKey(aKey)) {
				
				HashMap<String, Integer> currentNegReasonCountMap = AirlineNegReasonMap.get(aKey);
				
				if(!currentNegReasonCountMap.containsKey(aValue)) {
					
					currentNegReasonCountMap.put(aValue, 1);
					AirlineNegReasonMap.put(aKey, currentNegReasonCountMap);
		
				}
				else {
					int prevCount = currentNegReasonCountMap.get(aValue);
					currentNegReasonCountMap.put(aValue, prevCount+1);
					AirlineNegReasonMap.put(aKey, currentNegReasonCountMap);
				}
			}
		}
		
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);
		
		int counter = 0;
		context.write(new Text("Total Reasons"), new Text(String.valueOf(TotalReasonCount)));

		System.out.println(AirlineNegReasonMap.toString());
		for(Map.Entry<String, HashMap> airlineEntry : AirlineNegReasonMap.entrySet()) {
			
			HashMap<String, Integer> individualNegReasonCountMap = sortHashMapByValues(airlineEntry.getValue());
			
			String hashMapTop5NegReasons = "";
			
			for(Map.Entry<String, Integer> reasonEntry : individualNegReasonCountMap.entrySet()) {
				
				if(counter < 5) {
					
					hashMapTop5NegReasons += reasonEntry.getKey() + ", ";
					counter++;
				}
				else{ 
					counter = 0;
					break;
				}
			}
			context.write(new Text(airlineEntry.getKey()), new Text(hashMapTop5NegReasons));
			
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

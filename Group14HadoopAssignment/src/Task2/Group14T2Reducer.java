package Task2;

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

/** Task Done By : Aw Yee Cheong (1602328) **/
public class Group14T2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	HashMap<String, Integer> CountryCountMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
		for(IntWritable value : values) {
			
			String aKey = key.toString();
			int aValue = value.get();
			
			if(!CountryCountMap.containsKey(aKey)) {
				
				CountryCountMap.put(aKey, aValue);
			}
			else {
				
				int currentCount = CountryCountMap.get(aKey);
				CountryCountMap.put(aKey, currentCount+1);
			}
			
		}
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		super.cleanup(context);
		
		HashMap<String, Integer> individualNegReasonCountMap = sortHashMapByValues(CountryCountMap);
		
		String topCountry = individualNegReasonCountMap.entrySet().iterator().next().getKey();
		int numberOfComplaints = individualNegReasonCountMap.entrySet().iterator().next().getValue();
		
		context.write(new Text(topCountry), new IntWritable(numberOfComplaints));
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

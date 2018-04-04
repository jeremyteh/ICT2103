package Task3;

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
public class Group14T3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		String topCountry;
		int numberOfComplaints;
		
		for(Map.Entry<String, Integer> countryCountEntry : CountryCountMap.entrySet()) {
		
			topCountry = countryCountEntry.getKey();
			numberOfComplaints = countryCountEntry.getValue();
			
			context.write(new Text(topCountry), new IntWritable(numberOfComplaints));
		}
	}
}

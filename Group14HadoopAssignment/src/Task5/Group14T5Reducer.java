package Task5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Group14T5Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	HashMap<String, List<Float>> AirlineTurningPointMap = new HashMap<String, List<Float>>();
	List<Float> TurningPointList;
	
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
		
		for(FloatWritable value : values) {
			
			String aKey = key.toString();
			float aValue = value.get();
			
			if(!AirlineTurningPointMap.containsKey(aKey)) {
							
				TurningPointList = new ArrayList<Float>();
				
				TurningPointList.add(aValue);
				//System.out.println(key.toString() +" : " +  value.toString());
				AirlineTurningPointMap.put(aKey, TurningPointList);
			}
			else {
				TurningPointList = AirlineTurningPointMap.get(aKey);
				TurningPointList.add(aValue);
			}
			
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		
		for(Entry<String, List<Float>> airlineEntry : AirlineTurningPointMap.entrySet()) {
			
			TurningPointList = airlineEntry.getValue();
			Collections.sort(TurningPointList);
			
			int sizeOfList = airlineEntry.getValue().size();
			int index;
			float medianTurningPoint;
			
			if(sizeOfList % 2 == 0){
				index = sizeOfList / 2;
			}
			else{
				index = (sizeOfList + 1) / 2;
			}
			
			medianTurningPoint = TurningPointList.get(index);
			System.out.println(airlineEntry.getKey() + " : " + medianTurningPoint);
			context.write(new Text(airlineEntry.getKey()), new FloatWritable(medianTurningPoint));
		}
		
	}
	
}

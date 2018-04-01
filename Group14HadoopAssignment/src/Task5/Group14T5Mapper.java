package Task5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Group14T5Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	Text airline = new Text();
	FloatWritable trustingPointWritable = new FloatWritable();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			
			String airlineStr;
			float trustingPoint;
			if(parts.length == 27 && parts[8] != null && parts[16] != null) {
				
				if(parts[8].equals("_trust")){
					
				} else {
					trustingPoint = Float.valueOf(parts[8].toString());
					airlineStr = parts[16].trim();
					
					if((trustingPoint > 0 && trustingPoint <= 1) && airlineStr!=null && !airlineStr.isEmpty()) {					
						trustingPointWritable.set(trustingPoint);
						airline.set(airlineStr);
						context.write(airline, trustingPointWritable);
					}
				}
			}	
	}
	
}

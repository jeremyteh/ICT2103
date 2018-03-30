package Task1;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Group14T1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text airline = new Text();
	Text negativeReason = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String airlineStr;
			String negativeReasonStr;
			if(parts.length == 27 && parts[14].equals("negative") && parts[15] != null && parts[16] != null) {
				negativeReasonStr = parts[15].trim();
				airlineStr = parts[16].trim();
				if(negativeReasonStr!=null && !negativeReasonStr.isEmpty() && airlineStr!=null && !airlineStr.isEmpty()) {					
					negativeReason.set(negativeReasonStr);
					airline.set(airlineStr);
					context.write(airline, negativeReason);
				}
			}
	}
}

package Task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Group14T4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text airline = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String airlineStr;
			if(parts.length == 27 && parts[14].equals("positive") && parts[16] != null) {
				airlineStr = parts[16].trim();
				if(airlineStr!=null && !airlineStr.isEmpty()) {					
					airline.set(airlineStr);
					context.write(airline, new IntWritable(1));
				}
			}
	}
}

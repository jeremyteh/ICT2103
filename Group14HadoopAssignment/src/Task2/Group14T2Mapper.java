package Task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Group14T2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

	Text airline = new Text();
	Text country = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, Text>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String airlineStr;
			String countryStr;
			if(parts.length == 27 && parts[14].equals("negative") && parts[10] != null && parts[16] != null) {
				countryStr = parts[10].trim();
				airlineStr = parts[16].trim();
				if(countryStr!=null && !countryStr.isEmpty() && airlineStr!=null && !airlineStr.isEmpty()) {					
					country.set(countryStr);
					airline.set(airlineStr);
					context.write(airline, country);
				}
			}
	}
}
package Task6;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Teo Hwee Boon (1602086) **/
public class Group14T6Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException{
		
		String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		
		if (parts.length >= 22 && !parts[21].isEmpty() && parts[21].toLowerCase().matches(".*delay.*")){
			context.write(new Text("delay"), one);
		}
		else if(parts.length >= 22 && !parts[21].isEmpty() && parts[21].toLowerCase().matches(".*#sfo.*")){
			context.write(new Text("#sfo"), one);
		}
		
	}
}



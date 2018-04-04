package Task7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Md Zulfiqah (1602575) **/
public class Group14T7Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	IntWritable one = new IntWritable(1);
	int ipIndex = 0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
		
		if(key.get() == 0 && parts.length >= (ipIndex + 1)){
			for (int x = 0; x < parts.length; x++){
				if (parts[x].equals("_ip")){
					ipIndex = x;
					break;
				}
			}
		}
		else{
			if (parts.length >= (ipIndex + 1) && !parts[ipIndex].isEmpty()){
				context.write(new Text(parts[ipIndex]), one);
			}
		}
	}
}

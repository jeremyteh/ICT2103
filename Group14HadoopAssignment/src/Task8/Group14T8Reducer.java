package Task8;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Task Done By : Md Zulfiqah (1602575) **/
public class Group14T8Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println(key.toString());
		
		if(key.toString().equals("Result")){
			double count = 0;
			double noOfComparisions = 0;
			for(Text value : values){
				
				int number = Integer.parseInt(value.toString());
				count += number;
				noOfComparisions ++;
			}
			double percentage = (count / noOfComparisions) * 100;
			
			context.write(new Text("Percentage"), new Text(String.valueOf(percentage)));
		}
		else{
			context.write(new Text(key), new Text(""));
		}
	}
}

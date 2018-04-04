package Task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Task Done By : Aw Yee Cheong (1602328) **/
public class Group14T3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Hashtable<String, String> countryCodes = new Hashtable<>();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
				
		// We will put the ISO-3166-alpha.tsv to Distributed Cache in the driver class
		// so we can access to it here locally by its name
		BufferedReader br = new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
		
		String line = null;
		while(true) {
			line = br.readLine();
			if(line != null) {
				String parts[] = line.split("\t");
				countryCodes.put(parts[0], parts[1]);
			}else {
				break;// finished reading
			}
		}
		br.close();
	}

	Text country = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException{
			String[] parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String countryCodeStr;
			String countryName;
			if(parts.length >= 16 && parts[10] != null && parts[14].equals("negative") && (parts[15].equals("badflight") || parts[15].equals("CSProblem"))) {
				countryCodeStr = parts[10].trim();
				if(countryCodeStr!=null && !countryCodeStr.isEmpty()) {					
					countryName = countryCodes.get(countryCodeStr);
					if(countryName!=null && !countryName.isEmpty()) {	
						country.set(countryName);
						context.write(country, new IntWritable(1));
					}
				}
			}
	}
}

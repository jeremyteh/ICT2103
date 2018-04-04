package Task9Time;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Task Done By : Teo Hwee Boon (1602086) **/
public class Group14T9Reducer2 extends Reducer<Text, Text, Text, IntWritable> {
	
	int[] timeCount = new int[24];
	String[] timeofDay = new String[]{"00:00 to 01:00", "01:00 to 02:00", "02:00 to 03:00", "03:00 to 04:00", "04:00 to 05:00",
			"05:00 to 06:00", "06:00 to 07:00", "07:00 to 08:00", "08:00 to 09:00", "09:00 to 10:00", "10:00 to 11:00", 
			"11:00 to 12:00", "12:00 to 13:00", "13:00 to 14:00", "14:00 to 15:00", "15:00 to 16:00", "16:00 to 17:00", 
			"17:00 to 18:00", "18:00 to 19:00", "19:00 to 20:00", "20:00 to 21:00", "21:00 to 22:00", "22:00 to 23:00", 
			"23:00 to 00:00"};
		
	HashMap<String, Integer> TimeCountMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
		
		String aKey = key.toString();
		
		for(Text value : values) {
			
			String aValue = value.toString();
			convertStringToDate(aValue);			
		}
		
		
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		super.cleanup(context);
		
		for(int i=0; i < 24; i++) {
			
			context.write(new Text(timeofDay[i]), new IntWritable(timeCount[i]));
		}
	}



	public void convertStringToDate(String tweetDate) {
		
		DateFormat serverDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm"); 
		
		try {
			Date startDate = serverDateFormat.parse(tweetDate);
		    DateFormat displayDateFormat = new SimpleDateFormat("HH:mm"); 
		    String newTimeString = displayDateFormat.format(startDate);	    
		    
		    SimpleDateFormat parser = new SimpleDateFormat("HH:mm");
		    Date zero = parser.parse("00:00");
		    Date one = parser.parse("01:00");
			Date two = parser.parse("02:00");
			Date three = parser.parse("03:00");
			Date four = parser.parse("04:00");
			Date five = parser.parse("05:00");
			Date six = parser.parse("06:00");
			Date seven = parser.parse("07:00");
			Date eight = parser.parse("08:00");
			Date nine = parser.parse("09:00");
			Date ten = parser.parse("10:00");
			Date eleven = parser.parse("11:00");
			Date twelve = parser.parse("12:00");
			Date thirteen = parser.parse("13:00");
			Date fourteen = parser.parse("14:00");
			Date fifteen = parser.parse("15:00");
			Date sixteen = parser.parse("16:00");
			Date seventeen = parser.parse("17:00");
			Date eighteen = parser.parse("18:00");
			Date nineteen = parser.parse("19:00");
			Date twenty = parser.parse("20:00");
			Date twentyOne = parser.parse("21:00");
			Date twentyTwo = parser.parse("22:00");
			Date twentyThree = parser.parse("23:00");
		    
			Date userTime = parser.parse(newTimeString);
			
			if (userTime.after(zero) && userTime.before(one)) {
		    	
		    	timeCount[0] += 1;
		    }
		    else if (userTime.after(one) && userTime.before(two)) {
		    	
		    	timeCount[1] += 1;
		    }
		    else if (userTime.after(two) && userTime.before(three)) {
		    	
		    	timeCount[2] += 1;
		    }
		    else if (userTime.after(three) && userTime.before(four)) {
		    	
		    	timeCount[3] += 1;
		    }
		    else if (userTime.after(four) && userTime.before(five)) {
		    	
		    	timeCount[4] += 1;
		    }
		    else if (userTime.after(five) && userTime.before(six)) {
		    	
		    	timeCount[5] += 1;
		    }
		    else if (userTime.after(six) && userTime.before(seven)) {
		    	
		    	timeCount[6] += 1;
		    }
		    else if (userTime.after(seven) && userTime.before(eight)) {
		    	
		    	timeCount[7] += 1;
		    }
		    else if (userTime.after(eight) && userTime.before(nine)) {
		    	
		    	timeCount[8] += 1;
		    }
		    else if (userTime.after(nine) && userTime.before(ten)) {
		    	
		    	timeCount[9] += 1;
		    }
		    else if (userTime.after(ten) && userTime.before(eleven)) {
		    	
		    	timeCount[10] += 1;
		    }
		    else if (userTime.after(eleven) && userTime.before(twelve)) {
		    	
		    	timeCount[11] += 1;
		    }
		    else if (userTime.after(twelve) && userTime.before(thirteen)) {
		    	
		    	timeCount[12] += 1;
		    }
		    else if (userTime.after(thirteen) && userTime.before(fourteen)) {
		    	
		    	timeCount[13] += 1;
		    }
		    else if (userTime.after(fourteen) && userTime.before(fifteen)) {
		    	
		    	timeCount[14] += 1;
		    }
		    else if (userTime.after(fifteen) && userTime.before(sixteen)) {
		    	
		    	timeCount[15] += 1;
		    }
		    else if (userTime.after(sixteen) && userTime.before(seventeen)) {
		    	
		    	timeCount[16] += 1;
		    }
		    else if (userTime.after(seventeen) && userTime.before(eighteen)) {
		    	
		    	timeCount[17] += 1;
		    }
		    else if (userTime.after(eighteen) && userTime.before(nineteen)) {
		    	
		    	timeCount[18] += 1;
		    }
		    else if (userTime.after(nineteen) && userTime.before(twenty)) {
		    	
		    	timeCount[19] += 1;
		    }
		    else if (userTime.after(twenty) && userTime.before(twentyOne)) {
		    	
		    	timeCount[20] += 1;
		    }
		    else if (userTime.after(twentyOne) && userTime.before(twentyTwo)) {
		    	
		    	timeCount[21] += 1;
		    }
		    else if (userTime.after(twentyTwo) && userTime.before(twentyThree)) {
		    	
		    	timeCount[22] += 1;
		    }
		    else if (userTime.after(twentyThree) && userTime.before(zero)) {
		    	
		    	timeCount[23] += 1;
		    }
		}
		catch (ParseException e) {
		    e.printStackTrace();
		}
			
	}
}

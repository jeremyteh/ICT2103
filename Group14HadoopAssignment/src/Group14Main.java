import java.net.URI;
import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*Import packages*/
import Task1.Group14T1Mapper;
import Task1.Group14T1Reducer;
import Task2.Group14T2Mapper;
import Task2.Group14T2Reducer;
import Task3.Group14T3Mapper;
import Task3.Group14T3Reducer;
import Task4.Group14T4Mapper;
import Task4.Group14T4Reducer;
import Task5.Group14T5Mapper;
import Task5.Group14T5Reducer;
import Task6.Group14T6Mapper;
import Task6.Group14T6Reducer;
import Task7.Group14T7Mapper;
import Task7.Group14T7Reducer;
import Task8.Group14T8Mapper;
import Task8.Group14T8Combiner;
import Task8.Group14T8Reducer;
import Task9Channel.Group14T9Mapper;
import Task9Channel.Group14T9Reducer;
import Task9Time.Group14T9Mapper2;
import Task9Time.Group14T9Reducer2;


public class Group14Main {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StatisticsAnalysis");
		
		job.setJarByClass(Group14Main.class);			
		
		Path inputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input");
		Path outputPath;
		
		int userInput = 0;
		
			menu();
			Scanner scn = new Scanner(System.in);
			userInput = scn.nextInt();
						
			switch(userInput) {
				
				case 1:
					job.setJarByClass(Group14Main.class);
					job.setMapperClass(Group14T1Mapper.class);
					job.setReducerClass(Group14T1Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task1_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;				
				
				case 2:
					//Put this file to the distributed cache so we can use it to join
					job.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
					
					job.setMapperClass(Group14T2Mapper.class);
					job.setReducerClass(Group14T2Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task2_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);					
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 3:
					//Put this file to the distributed cache so we can use it to join
					job.addCacheFile(new URI("hdfs://localhost:9000/user/phamvanvung/airline/ISO-3166-alpha3.tsv"));
					
					job.setMapperClass(Group14T3Mapper.class);
					job.setReducerClass(Group14T3Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task3_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 4:
					job.setMapperClass(Group14T4Mapper.class);
					job.setReducerClass(Group14T4Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task4_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 5:
					job.setMapperClass(Group14T5Mapper.class);
					job.setReducerClass(Group14T5Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(FloatWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task5_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 6:
					job.setMapperClass(Group14T6Mapper.class);
					job.setReducerClass(Group14T6Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task6_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 7:
					job.setMapperClass(Group14T7Mapper.class);
					job.setReducerClass(Group14T7Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task7_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 8:
					job.setMapperClass(Group14T8Mapper.class);
					job.setCombinerClass(Group14T8Combiner.class);
					job.setReducerClass(Group14T8Reducer.class);
					
					job.setMapOutputKeyClass(Text.class);
					job.setMapOutputValueClass(Text.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task8_"
							+new Date().getTime());//use run-time as output folder
					
					job.setMapOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 9:
					
					job.setMapperClass(Group14T9Mapper.class);
					job.setReducerClass(Group14T9Reducer.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task9_"
							+new Date().getTime());//use run-time as output folder
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				case 10:
					
					job.setMapperClass(Group14T9Mapper2.class);
					job.setReducerClass(Group14T9Reducer2.class);
					
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(IntWritable.class);
					
					outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task10_"
							+new Date().getTime());//use run-time as output folder
					
					job.setMapOutputValueClass(Text.class);
					
					FileInputFormat.addInputPath(job, inputPath);
					FileOutputFormat.setOutputPath(job, outputPath);
					
					System.exit((job.waitForCompletion(true))?0:1);
					break;
					
				default:
					break;
			}		
	}
	
	public static void menu() {
		System.out.println("Welcome to Hadoop Twitter Analysis System");
		System.out.println("1. Top 5 Negative Reasons of each airline and Total Negative Reasons");
		System.out.println("2. People from which country comaplain the most");
		System.out.println("3. Negative Comments \"badflight\" or \"CSProblem\" ");
		System.out.println("4. Top 3 Airlines Most Number of Positive Tweets");
		System.out.println("5. Airline Trust value (Median)");
		System.out.println("6. Number of delayed flights, with keywords \"delayed\" or \"#SFO\"");
		System.out.println("7. Top 5 Negative Reasons of each airline and Total Negative Reasons");
		System.out.println("8. Sentiment Value Percentage Similarity");
		System.out.println("9. Channel Popularity");
		System.out.println("10. Frequency of posts during different times of day");
		System.out.println("");
		System.out.print("Please enter an option between 1 and 10: ");
	}
	
}

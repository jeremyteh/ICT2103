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

import Task7.*;
import Task8.Group14T8Mapper;
import Task8.Group14T8Reducer;
import Task9One.Group14T9Mapper;
import Task9One.Group14T9Reducer;
import Task9Two.Group14T9Mapper2;
import Task9Two.Group14T9Reducer2;


public class Group14Main {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StatisticsAnalysis");
		
		job.setJarByClass(Group14Main.class);			
		
		Scanner input = new Scanner(System.in);
		
		System.out.print("Please enter an option: ");
		int userInput = input.nextInt();
		
		Path inputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/");
		Path outputPath;
		
		switch(userInput) {
			
			case 1: 
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
				job.setReducerClass(Group14T8Reducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				
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
	
}

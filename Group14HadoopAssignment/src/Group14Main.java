import java.util.Date;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Task1.Group14T1Mapper;
import Task1.Group14T1Reducer;

public class Group14Main {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StatisticsAnalysis");
		
		job.setJarByClass(Group14Main.class);			
		
		Scanner input = new Scanner(System.in);
		
		System.out.print("Please enter an option: ");
		int userInput = input.nextInt();
		
		switch(userInput) {
			
			case 1: 
				job.setMapperClass(Group14T1Mapper.class);
				job.setReducerClass(Group14T1Reducer.class);
				
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				Path inputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/input/");
				Path outputPath = new Path("hdfs://localhost:9000/user/phamvanvung/airline/output/Group14Task1_"
						+new Date().getTime());//use run-time as output folder
				
				FileInputFormat.addInputPath(job, inputPath);
				FileOutputFormat.setOutputPath(job, outputPath);
				
				System.exit((job.waitForCompletion(true))?0:1);
				break;
				
			case 2:
				break;
				
			default:
				break;
		}
	}
	
}

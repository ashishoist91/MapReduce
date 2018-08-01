package com.ashish.mapReduce.bigram;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ashish.mapReduce.invertedIndex.InvertedIndex;
import com.ashish.mapReduce.invertedIndex.Map;
import com.ashish.mapReduce.invertedIndex.Reduce;
import com.ashish.mapReduce.wordCount.WordCountWithTool;

public class Bigram  extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Bigram(), args);
		System.exit(exitCode);

	}
	
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage : <Input Path> <Output Path>");
			System.exit(0);
		}
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setJobName("Bigram");
		

		job.setMapOutputKeyClass(TextPair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		return job.waitForCompletion(true)?0:1;
	}

}

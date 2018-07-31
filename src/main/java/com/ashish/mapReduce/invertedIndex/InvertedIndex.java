package com.ashish.mapReduce.invertedIndex;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ashish.mapReduce.wordCount.WordCountWithTool;

public class InvertedIndex extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new WordCountWithTool(), args);
		System.exit(exitCode);

	}

	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid Command");
			System.err.println("Usage : <Input Path> <Output Path>");
			System.exit(0);
		}
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(InvertedIndex.class);
		
		job.setJobName("invertedIndex");
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileInputFormat.setInputDirRecursive(job, true);
		
		return job.waitForCompletion(true)?0:1;
	}

}

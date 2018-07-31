package com.ashish.mapReduce.wordCount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	String line = value.toString();
	String[] words=line.split(",");
	for(String word: words )
	{
	      Text outputKey = new Text(word.toUpperCase().trim());
	      IntWritable outputValue = new IntWritable(1);
	      context.write(outputKey, outputValue);
	}
	}

}

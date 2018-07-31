package com.ashish.mapReduce.invertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	Text word = new Text();
	Text fileName = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		FileSplit  currentSplit = (FileSplit) context.getInputSplit();
		String   fileNameStr = currentSplit.getPath().getName();
		fileName = new Text(fileNameStr);
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			context.write(word, fileName);
		}

	}

}

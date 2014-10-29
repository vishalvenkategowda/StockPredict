package com.fall2014.bigdata.stockpredict.maptask;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ReadDataMapTask extends MapReduceBase 
				implements Mapper<LongWritable, Text, Text, IntWritable>{
	
	private String[] matchWords = { "Apple" };	
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		int count =0;
		int flag = 0;
						
		String line = value.toString();
		String[] words = line.split("-|\\,|\\ |\\:|\\#");
		String[] visitedwords = {" ", " ", " ", " "};
		
		for(String word1: words){
			for(String  word2: matchWords){				
				
				if(word1.equalsIgnoreCase(word2)){
					
					for(int i = 0; i < 3; i++){
						if(visitedwords[i].equalsIgnoreCase(word2))
							flag = 1;
					}
					if(flag == 0){
						visitedwords[count] = word2;
						count++;
						output.collect(new Text(word2), new IntWritable(1));		
						break;	
					}					
				}
				else				
					output.collect(new Text(word2), new IntWritable(0));
			}
			
		}
		
	}

}

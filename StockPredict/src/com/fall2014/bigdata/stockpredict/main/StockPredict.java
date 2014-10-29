package com.fall2014.bigdata.stockpredict.main;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.BasicConfigurator;

import com.fall2014.bigdata.stockpredict.maptask.ReadDataMapTask;
import com.fall2014.bigdata.stockpredict.reducetask.ReadDataReduceTask;

public class StockPredict {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		
		BasicConfigurator.configure();
		
		if(args.length!=2){
			System.err.println("Usage: StockPredict <input_path> <output_Path>");
			System.exit(-1);
		}
		
		JobConf jobConf = new JobConf(StockPredict.class);
		jobConf.setJobName("Read Tweets from the txt file");
		
		FileInputFormat.addInputPath(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		
		jobConf.setMapperClass(ReadDataMapTask.class);
		jobConf.setReducerClass(ReadDataReduceTask.class);
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(jobConf);
		
		
	}

}

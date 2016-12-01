package com.chengjungao.solr.mr;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AppPartitioner extends Partitioner<IntWritable, Text> {
    

    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return  key.get() % numPartitions;
    }

}
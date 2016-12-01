package com.chengjungao.solr.mr;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chengjungao.solr.util.ConstantDefine;

public class JobRun extends Configured implements Tool{
     private Logger  logger = LoggerFactory.getLogger(JobRun.class);
	 private Configuration config;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobRun job  = new JobRun();
		ToolRunner.run(job, args);
	}
	
	public int run(String [] args) throws Exception{
		
		config = new Configuration();
		String configFile =args[0];
		File file = new File(configFile);
		config.addResource(new FileInputStream(file));
		int maxSplitSize = config.getInt(ConstantDefine.maxSplitSize, 128);
		int minSplitSize = config.getInt(ConstantDefine.minSplitSize, 128);
		int taskNumber = Integer.parseInt(args[1]);
		Job job = null;
		try {
	        System.out.println("任务开始！");
	        
	        Path inputPath = new Path(config.get(ConstantDefine.inputPath));
	        
	        //如果输出路径存在则删除
	        String outputDir = config.get(ConstantDefine.outputDir);
	        logger.info("reduce输出路径设置为{}.",outputDir);
	        System.out.println("reduce输出路径设置为"+outputDir);
	        Path path = new Path(outputDir);
	        FileSystem fs = path.getFileSystem(config);
	        fs.deleteOnExit(path);
	        fs.close();
	        
	        Path classPath = new Path(config.get(ConstantDefine.jobClassPath));
	        FileSystem classfs = classPath.getFileSystem(config);
	        job = Job.getInstance(config);
	        for (FileStatus filestatus : classfs.listStatus(classPath)) {
	        	  job.addFileToClassPath(filestatus.getPath());
			}
	        job.setJobName(config.get(ConstantDefine.jobName));
	        job.setJarByClass(JobRun.class);
	        
	        job.setMapperClass(SolrMapper.class);
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        
	        job.setInputFormatClass(TextInputFormat.class); //任务输入
	        TextInputFormat.setMaxInputSplitSize(job, 1024L*1024*maxSplitSize);
	        TextInputFormat.setMinInputSplitSize(job,  1024L*1024*minSplitSize);
	        TextInputFormat.addInputPath(job, inputPath);
	        
	        
	        job.setOutputFormatClass(TextOutputFormat.class);
	        TextOutputFormat.setOutputPath(job, path);
	        job.setReducerClass(SolrReduce.class);
	        job.setNumReduceTasks(taskNumber);
	        job.setPartitionerClass(AppPartitioner.class);
	        boolean  flag = job.waitForCompletion(true);
	        JobStatus status = job.getStatus();
	        status.getMapProgress();
	        status.getReduceProgress();
	        logger.info("job执行完成,返回状态{}.", flag);
	        Counters counters = job.getCounters();
	        long totalCounterNumber = counters.findCounter(SolrReduce.COUNTER.TOTAL_RECORDN_NUMBER).getValue();
	        long commitCounterNumber = counters.findCounter(SolrReduce.COUNTER.COMMIT_DOC_NUMBER).getValue();
	        long errorCounterNumber = counters.findCounter(SolrReduce.COUNTER.PARSE_ERROR_NUMBER).getValue();
	        logger.info("记录总数为{}条,提交文档数据为{}条,解析错误数据为{}条。",totalCounterNumber,commitCounterNumber,errorCounterNumber);
	        
		}catch (Exception e) {
			  System.out.println(e.getMessage()); //将异常打印出
	       }finally {
	             SystemDefaultHttpClient httpClient = HttpClientFactory.createHttpClient();
	             CloudSolrClient cloudSolrClient = new CloudSolrClient(config.get(ConstantDefine.zookeeperAddress),httpClient);
	             cloudSolrClient.setDefaultCollection(config.get(ConstantDefine.collectoinName));
	             System.out.println("等待提交.....................");
	             long startTime = System.currentTimeMillis();
	             cloudSolrClient.commit(false,true,false);
	             long endTime = System.currentTimeMillis();
	             logger.info("commit耗时：{}ms",endTime - startTime);
	             cloudSolrClient.close();
	        }
		return 0;
	}

}

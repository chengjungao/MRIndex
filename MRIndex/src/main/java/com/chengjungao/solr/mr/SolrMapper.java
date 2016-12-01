package com.chengjungao.solr.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil.HttpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chengjungao.solr.util.ConstantDefine;
import com.chengjungao.solr.util.DocRoute;
import com.chengjungao.solr.util.ParseText;
import com.chengjungao.solr.util.Split;

public class SolrMapper extends Mapper<WritableComparable<?>, Text, IntWritable, Text>{
    
    private Logger logger = LoggerFactory.getLogger(SolrMapper.class);
    
    private CloudSolrClient cloudClient;
    
    private String collectoinName;//集合名字
    private String[] columns;//集合列名集合   例如:column1,column2,column3
    private String columnSplit;//集合列名集合  的分隔符
    private String dataSplit;//集合数据  的分隔符
    
    private int limitprint = 0;
    
    private String charSet;//hdfs文件字符编码
    
    Configuration config = null ; 
    
    @Override
    protected void setup(Context context)throws IOException, InterruptedException {
    	
    	config = context.getConfiguration();
        
        String address = config.get(ConstantDefine.zookeeperAddress);
        logger.info("初始化zookeeper地址{}...", address);
        
        collectoinName = config.get(ConstantDefine.collectoinName);
        logger.info("创建集合名字{}.", collectoinName);
        
        String cloumnsStr = config.get(ConstantDefine.collectionColumns);
        logger.info("field集合{}.", cloumnsStr);        
        
        columnSplit = config.get(ConstantDefine.collectionColumnsSplit);
        logger.info("field切割符号【{}】.", columnSplit);
        
        dataSplit = config.get(ConstantDefine.collectionDataSplit);
        logger.info("数据切割符号【{}】.", dataSplit);
        
        columns = Split.split(cloumnsStr, columnSplit);
        
        charSet = config.get(ConstantDefine.charSet);
        
        
        SystemDefaultHttpClient  client = HttpClientFactory.createHttpClient();
        cloudClient = new CloudSolrClient(address,client);
        cloudClient.connect();
        
        
        logger.info("连接zookeeper{}成功...",address);
        
    }
    

    @Override
    protected void map(WritableComparable<?> key, Text value,Context context)throws IOException, InterruptedException {
        //如果需要先转换文本编码
        String linestr = value.toString();
        if (charSet!=null && !charSet.equals("")) {//需要转换编码
            
            linestr = new String (value.getBytes(),charSet); //获得字符串
            
            linestr = new String (linestr.getBytes("UTF-8"),"UTF-8");//将编码转换成utf-8
            
        }
        //每个mapper打印5条数据
        if(limitprint < 5){
            String[] datas = ParseText.parseLine(linestr, dataSplit);
            for (int i = 0; i < datas.length; i++) {
                logger.info("{}", datas[i]);
            }
            logger.info("--------------------");
        }
        limitprint++;
        //提取solr documentId
        String docId = ParseText.getDocId(linestr, columns,config);
        if (docId.equals("")) {//  如果主键不存在
            System.out.println("文本偏移量:"+key+"内容:"+ linestr);
        }
        //无论数据是否有问题 都输出到reduce  在reduce端做解析错误判断
        String shardId = DocRoute.getShardID(cloudClient, collectoinName, docId);
        context.write(new IntWritable(Integer.parseInt(shardId)), new Text(linestr));    
        
        
    
    }



    @Override
    protected void cleanup(Context context)throws IOException, InterruptedException {
        cloudClient.close();
    }
}
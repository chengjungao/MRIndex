package com.chengjungao.solr.mr;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil.HttpClientFactory;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chengjungao.solr.util.ConstantDefine;
import com.chengjungao.solr.util.ParseText;
import com.chengjungao.solr.util.Split;



public class SolrReduce extends Reducer<IntWritable, Text, NullWritable, Text>{
    
    private Logger logger = LoggerFactory.getLogger(SolrReduce.class);

    private CloudSolrClient cloudClient;//集群server
    private HttpSolrClient client;
    
    private String sharedLeaderUrl;// 
    
    private List<SolrInputDocument> documents;
    
    private String[] columns;//集合列名集合   例如:cloumn1,cloumn2,cloumn3
    private String [] columns_types;//列的类型
    Configuration config = null ;
    
    SystemDefaultHttpClient httpClient = HttpClientFactory.createHttpClient();
    
    
    
    @Override
    protected void setup(Context context)throws IOException, InterruptedException {
    	 config = context.getConfiguration();
    	
        columns = Split.split(config.get(ConstantDefine.collectionColumns), config.get(ConstantDefine.collectionColumnsSplit));
       
        columns_types = Split.split(config.get(ConstantDefine.columnsTypes), config.get(ConstantDefine.collectionColumnsSplit) );
        
        cloudClient = new CloudSolrClient(config.get(ConstantDefine.zookeeperAddress),httpClient);
        
        cloudClient.setDefaultCollection(config.get(ConstantDefine.collectoinName));
        
        cloudClient.connect();
        
        logger.info("连接zookeeper成功...");
        
        documents = new ArrayList<SolrInputDocument>(config.getInt(ConstantDefine.collectionCommitNumber,10000));
        
    }

    /**
     * @param key 集合分片的id  如1表示shared1
     */
    @Override
    protected void reduce(IntWritable key, Iterable<Text> valeus,Context context)throws IOException, InterruptedException {
         ZkStateReader reader = cloudClient.getZkStateReader();
         try {
             sharedLeaderUrl = reader.getLeaderUrl(config.get(ConstantDefine.collectoinName), "shard"+key, 10000);
             logger.info("sharedLeaderUrl======>{}", sharedLeaderUrl);
             client = new HttpSolrClient(sharedLeaderUrl,httpClient);
             client.setUseMultiPartPost(true);
        } catch (Exception e) {
            logger.error("{}", key,e);
        }
         
        for (Text value : valeus) {
            SolrInputDocument document = ParseText.getDocument(value.toString(), columns, columns_types,config);
            if(document != null){
                documents.add(document);
                context.getCounter(COUNTER.COMMIT_DOC_NUMBER).increment(1);
            }else{
                context.getCounter(COUNTER.PARSE_ERROR_NUMBER).increment(1);
            }
            
            if(documents.size() !=0 && documents.size() % config.getInt(ConstantDefine.collectionCommitNumber,10000) == 0){
                try {
                    logger.info("添加文档{}条...",documents.size());
                    long startTime = System.currentTimeMillis();
                    UpdateResponse resonpse  = client.add(documents);
                    long endTime = System.currentTimeMillis();
                    logger.info("solrj记录耗时：{}",resonpse.getElapsedTime());
                    logger.info("本次add耗时：{}ms",endTime - startTime);
                    context.getCounter(COUNTER.ADD_DOC_TIME).increment(endTime - startTime);
                    documents.clear();
                } catch (SolrServerException e) {
                    logger.error("提交文档错误", e);
                }
            }
            context.getCounter(COUNTER.TOTAL_RECORDN_NUMBER).increment(1);
        }
        try {
            if (!documents.isEmpty()&&documents.size()!=0) {
                client.add(documents);
                logger.info("添加文档{}条.",documents.size());
                documents.clear();
            }
        } catch (SolrServerException e) {
            logger.error("提交文档错误", e);
        }
    }


    @Override
    protected void cleanup(Context context)throws IOException, InterruptedException {
        if (client != null) {
            client.close();
        }
        cloudClient.close();
    }
    
    public static enum COUNTER {
        TOTAL_RECORDN_NUMBER,//总的记录数
        COMMIT_DOC_NUMBER,//提交文档数量
        PARSE_ERROR_NUMBER,//解析错误记录数
        ADD_DOC_TIME //文档提交时间  单位ms
    }
    
}

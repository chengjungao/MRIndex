package com.chengjungao.solr.util;

public  class  ConstantDefine {
    
    public final static String inputPath = "input.path"; 
    
    public static final String jobClassPath = "job.class.path";                            //类路径
  
    public static final String outputDir = "output.dir";                                //输出文件路径
    
    public static final String taskNumber = "task.number";
    
    public static final String zookeeperAddress = "zookeeper.address";                    //zookeeper集群地址
    
    public static final String collectionCommitNumber = "collection.commit.number";     //每次添加文档数量
    
    public static final String collectionColumns = "collection.columns";                //集合的列集合
    
    public static final String collectionFieldIdIndex = "collection.fieldId.index";        //集合主键列
    
    public static final String collectionColumnsSplit = "collection.columns.split";        //集合列的切割符号
    
    public static final String collectionDataSplit = "collection.data.split";            //集合数据切割符号
    
    public static final String collectionCreateMode = "collection.create.mode";            //提交模式  update和add
   
    public static final String collectoinName = "collection.name";                      //集合的名字
    
    public static final String columnsTypes="columns.type"; //列类型
    
    public static final String HbaseColumns = "hbase.columns";                //hbase的列
   
    public static final String hbasetablename = "hbase.table";                //hbase的列
    
    public static final String familyName="hbase.family";                   //hbase 列簇
    
    public static final String jobName = "job.name";
    
    public static final String isCommit = "solr.iscommit"; //建完索引后立刻commit
    
    public static final String isCreateHtable = "create.hbase.table"; //是否创建hbase table
    
    public static final String hbasePartionNum = "hbase.partion.number"; //hbase 预分区数量
    
    public static final String solrHbaseRowkey = "solr.hbase.rowkey"; //solr 中存储hbase rowkey的字段名
    
    public static final String rowKeyMethod = "rowKey.method"; //创建rowKey的方法
    
    public static final String maxSplitSize = "mapred.min.split.size"; //最大分片大小
    
    public static final String minSplitSize = "mapred.max.split.size"; //最小分片大小
    
    public static final String charSet = "hdfs.charset"; //hdfs文件编码.
    
    
}

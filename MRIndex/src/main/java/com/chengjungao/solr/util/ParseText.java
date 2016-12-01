package com.chengjungao.solr.util;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ParseText {
    
    private static final Logger logger = LoggerFactory.getLogger(ParseText.class);
    
    /**
     * 获得文档id
     * @param text
     * @param columns
     * @return
     */
    public static String getDocId(String text, String[] columns,Configuration config){
        String[] datas = parseLine(text, config.get(ConstantDefine.collectionDataSplit));
        boolean model = config.getBoolean(ConstantDefine.collectionCreateMode,false);
        String  id = "";
        if(model){//非增量
        String[] fieldIdIndex =  Split.split(config.get(ConstantDefine.collectionFieldIdIndex),config.get(ConstantDefine.collectionColumnsSplit));
        String key = ""; 
        for (int i = 0; i < fieldIdIndex.length; i++) {
             try {
               if (Integer.parseInt(fieldIdIndex[i]) > datas.length-1) {
                key = "";
                break;
              }
             key += datas[Integer.parseInt(fieldIdIndex[i])];
             } catch (NumberFormatException e) {
                    // TODO: handle exception
                 throw new RuntimeException("主键位置为非法字符串.");
            }
         }
        id = MD5Util.MD5TO16(key);
    }
    else{//增量
        id = MD5Util.MD5TO16(text);
    }
    return id;
    }
    
    /**
     * 根据索引数据文本返回索引文档
     * @param text  索引数据文本
     * @param cloumnsStr 列字段名集合
     * @param cloumnSplit 列名分隔符
     * @param dataSplit 数据文本分隔符
     * @param fieldIdIndex 主键位置
     * @return  返回文档
     */
    public static SolrInputDocument getDocument(String text, String[] columns, String[] types,Configuration config){
        SolrInputDocument doc = null;
        try {
            String[] datas = parseLine(text, config.get(ConstantDefine.collectionDataSplit));
            doc = new SolrInputDocument();
            String  id = getDocId(text,columns,config);
        
        doc.addField("id", id); //添加主键
        for (int index = 0; index < columns.length ; index ++ ) {
                String[] ds = columns[index].split("=");
                int columnsIndex = Integer.parseInt(ds[0]);
                String columnName = ds[1];
                doc.addField(columnName,  parseValue(datas[columnsIndex].trim(), types[index],index));
         }
        
        } catch (Exception e) {
            doc = null;
            e.printStackTrace();
            logger.error("解析数据错误:{}错误信息:{}", text, e);
        }
        return doc;
        
    }
    
    public static String[] parseLine(String text, String dataSplit){
        if(dataSplit.startsWith("\\u")){
            dataSplit = dataSplit.substring(2);
            dataSplit = "" + (char)Integer.parseInt(dataSplit,16);
        }
        return Split.split(text, dataSplit);
    }
    
    /**
     * 将字段值转换成对应的类型
     * @param value
     * @param type
     * @return
     */
    public static Object parseValue(String value,String type,int index){
    
        value = value.trim();
        Object obj = null;
        switch (type) {
        case "int":obj = Integer.parseInt(value);
             break;
        case "long":obj = Long.parseLong(value);
             break;
        case "double":obj = Double.parseDouble(value);
             break;
        case "String":obj = value;
             break;
        case "date":obj = convert2Date(value);
             break;
             
        default:
            obj = value;
            break;
        }
        return obj;
        
    }
    
    
    /**
     * 将日期字符串格式化为solr date类型 yyyy-MM-dd'T'HH:mm:ssS'Z'
     * @param str 日期字符串 ,格式： "yyyyMMdd","yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
                "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyyMMddHHmmss"
     * @return
     */
    public static String convert2Date(String str) {
        SimpleDateFormat YMDHMS = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssS'Z'");
        String[] DATE_PATTERN = { "yyyyMMdd","yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss",
                "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyyMMddHHmmss" };
        try {
            return YMDHMS.format(DateUtils.parseDate(str, DATE_PATTERN));
        } catch (Exception e) {
            
            return null;
        }
    }
    
    public static void main(String[] args) {
        String str = "2016-06-12";
        System.out.println(convert2Date(str));
    }
}

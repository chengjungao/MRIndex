package com.chengjungao.solr.util;

import java.util.Collection;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter.Range;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

public class DocRoute {
  static DocCollection collection =null;
  
  
  public static void getDocCollection(CloudSolrClient cloudSolrClient,String collectionName){
	  collection = ZkStateReader.getCollectionLive(cloudSolrClient.getZkStateReader(), collectionName);
	  
  }
  
  private static Slice hashToSlice (int hash){
	  Collection<Slice>  slices  = collection.getSlices();
	  for (Slice slice : slices) {
		Range range = slice.getRange();
		if (range != null && range.includes(hash)) {
			return slice;
		}
	  }
	 throw new  RuntimeException("solr异常！"); 
  }
  
  public static String getShardID(CloudSolrClient cloudSolrClient,String collectionName,String key){
	  
	  if(collection == null){
		  getDocCollection(cloudSolrClient,collectionName);
		  
	  }
	  CompositeIdRouter docRouter = (CompositeIdRouter) collection.getRouter();
	  int hash = docRouter.sliceHash(key, null, null, null);
	  Slice slice = hashToSlice(hash);
	  return slice.getName().substring(5);
	  
  }
	
	
}

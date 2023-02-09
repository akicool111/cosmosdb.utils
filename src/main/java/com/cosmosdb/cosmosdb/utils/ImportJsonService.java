package com.cosmosdb.cosmosdb.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.mongodb.DBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

@Service
public class ImportJsonService {

    @Autowired
    private  MongoTemplate mongo;
    
   
	private static String cosmosdbCollections;
	

	private static String cosmosdbCollectionsBackup;
	
	
	 @Value("${cosmosdb.collections}")
    public  void setCosmosdbCollections(String cosmosdbCollections) {
		ImportJsonService.cosmosdbCollections = cosmosdbCollections;
	}

	@Value("${cosmosdb.collections.backup}")
	public  void setCosmosdbCollectionsBackup(String cosmosdbCollectionsBackup) {
		ImportJsonService.cosmosdbCollectionsBackup = cosmosdbCollectionsBackup;
	}

	private  List<Document> generateMongoDocs_1(List<String> lines) {
        List<Document> docs = new ArrayList<>();
        for (String json : lines) { 
            docs.add(Document.parse(json));
        }
        return docs;
    }
	
	private boolean validateCollectionName(String input)
	{
		
		if (input.matches("^[a-zA-Z0-9_]+$")) {
		   // System.out.println("Collection "+input+" contains only letters, numbers, and underscore");
		    return true;
		} else {
		    System.out.println("Error : Collection "+input+"  contains characters other than letters, numbers, and underscore");
		    return false;
		}
	}
    
    private  List<Document> generateMongoDocs(String fileName) throws StreamReadException, DatabindException, IOException {
    		final ObjectMapper objectMapper = new ObjectMapper();
    	
    	objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    	objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CAMEL_CASE);
    	objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    	List<org.bson.Document> docs=(List<org.bson.Document>)objectMapper.readValue(
    	        new File(fileName), 
    	        new TypeReference<List<org.bson.Document>>(){});
        return docs;
    }
    
    private  boolean checkCollectionExists(String collectionName)
    {
    	if (mongo.collectionExists(collectionName)) {
    	    //System.out.println("Collection " + collectionName + " exists.");
    	    return true;
    	} else {
    	   // System.out.println("Collection " + collectionName + " does not exist.");
    	    return false;
    	}
    }
    
    private  int insertInto(String collection, List<Document> mongoDocs) {
        try {
        	  
            Collection<Document> inserts = mongo.insert(mongoDocs, collection);
            return inserts.size();
        } catch (DataIntegrityViolationException e) {
            if (e.getCause() instanceof MongoBulkWriteException) {
                return ((MongoBulkWriteException) e.getCause())
                  .getWriteResult()
                  .getInsertedCount();
            }
            return 0;
        }
    }
    
    private  boolean deleteFrom(String collection,String backupCollection) {
        try {
           
        	Boolean collectionMasterExistFlag=checkCollectionExists(collection);
        	if(!collectionMasterExistFlag)
        	{
        		System.out.println(collection+" : This Master  Collection does not  exist ,Creating one");
        		
        		mongo.createCollection(collection);
        		return true;
        	}
        	
        	
        	Boolean collectionExistFlag=checkCollectionExists(backupCollection);
        	if(collectionExistFlag)
        	{
        		System.out.println(backupCollection+" : This Backup Collection Already exist ,use different collection name");
        		System.out.println();
        		return false;
        	}
        	mongo.createCollection(backupCollection);
        	
        	
        	MongoCollection<Document> sourceCollection = mongo.getCollection(collection);
        	FindIterable<Document> sourceData = sourceCollection.find();
        	
        	
        	List<Document> sourceDataList = new ArrayList<>();
        	for (Document document : sourceData) {
        	    sourceDataList.add(document);
        	}
        //	System.out.println("sourceDataList :"+sourceDataList.size());
        	//destinationCollection.insertMany(sourceDataList);
        	mongo.insert(sourceDataList,backupCollection );
			/*
			 * for (Document data : sourceData) { System.out.println("data :"+data);
			 * destinationCollection.insertOne(data); }
			 */
        	//mongo.createCollection("TestDocument06022023");
        	//mongo.find 
        	//MongoCollection<Document> list= mongo.getCollection("TestDocument");
        	//mongo.insert(list,"TestDocument06022023");
        	mongo.dropCollection(collection);
        	mongo.createCollection(collection);
           return true;
        } catch (DataIntegrityViolationException e) {
        	System.out.println("inside catch of deleteFrom");
        
            if (e.getCause() instanceof MongoBulkWriteException) {
               e.printStackTrace();
            }
            return false;
        }
    }
    
	/*
	 * private boolean isCollectionExists( String collectionName) {
	 * 
	 * DBCollection table = db.getCollection(collectionName); return
	 * (table.count()>0)?true:false; }
	 */
    
    
    private  String importTo(String collection,String fileName,String backupCollection) throws StreamReadException, DatabindException, IOException {
      // System.out.println("Inside importTo function");
     
        List<Document> mongoDocs = generateMongoDocs(fileName);
        Boolean deleteFromFlag= deleteFrom(collection,backupCollection);
        if(deleteFromFlag)
        {
        	System.out.println("for "+collection+" backup is successfully created as "+backupCollection+" and deleted the collection ");
        	
        	int inserts = insertInto(collection, mongoDocs);
            return "Success : "+inserts + "/" + mongoDocs.size();
        }
        return "Error : Backup and delete operation Failed";
        
    }
    
    private  void  importServiceRunner()  throws StreamReadException, DatabindException, IOException
    {
    	
    	System.out.println("processing started : importServiceRunner : \n");
    	
    	if(cosmosdbCollections==null || cosmosdbCollectionsBackup==null)
    	{
    		System.out.println("Error : Please check properties file configuration");
    		return;
    	}
    	List<String> cosmosdbCollectionsList=Arrays.asList(cosmosdbCollections.split(","));
		List<String> cosmosdbCollectionsBackupList=Arrays.asList(cosmosdbCollectionsBackup.split(","));
		if(cosmosdbCollectionsList.size()!=cosmosdbCollectionsBackupList.size())
		{
			System.out.println("Error : Master collection and backup collection count is not matching ,Please check properties file");
			return;
		}
		
		for(int i=0;i<cosmosdbCollectionsList.size();i++)
		{
			if(!validateCollectionName(cosmosdbCollectionsList.get(i)))
			{
				return;
			}
			if(!validateCollectionName(cosmosdbCollectionsBackupList.get(i)))
			{
				return;
			}
			
		}
		for(int i=0;i<cosmosdbCollectionsList.size();i++)
		{
			Boolean collectionExistFlag=checkCollectionExists(cosmosdbCollectionsBackupList.get(i));
        	if(collectionExistFlag)
        	{
        		System.out.println("Error : "+cosmosdbCollectionsBackupList.get(i)+" : This Backup Collection Already exist ,use different collection name");
        		System.out.println();
        		return;
        	}
		}
		for(int i=0;i<cosmosdbCollectionsList.size();i++)
		{
			File f = new File(cosmosdbCollectionsList.get(i)+".json");
			
			if(!f.exists())
			{
				System.out.println("Error : "+cosmosdbCollectionsList.get(i)+".json file does not exist");
				System.out.println();
				return;
			}
		}
		
		for(int i=0;i<cosmosdbCollectionsList.size();i++)
		{
			System.out.println("processing collection :"+cosmosdbCollectionsList.get(i));
			
		
			
			String output=importTo(cosmosdbCollectionsList.get(i), cosmosdbCollectionsList.get(i)+".json",cosmosdbCollectionsBackupList.get(i));
	    	//System.out.println("importTo output :"+output);
			System.out.println("processing of "+cosmosdbCollectionsList.get(i)+" completed. Imported data count : "+output+"\n");
		}
		
		System.out.println("processing end : importServiceRunner");
		
    	
    }
    
    @EventListener(ApplicationReadyEvent.class)
	public void doSomethingAfterStartup() {
	    System.out.println("hello Utils, I have just started up");
	    
	    try {
			importServiceRunner();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
}
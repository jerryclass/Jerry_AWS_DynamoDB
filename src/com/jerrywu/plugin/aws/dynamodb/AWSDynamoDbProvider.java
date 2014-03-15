
package com.jerrywu.plugin.aws.dynamodb;


import java.io.IOException;
import java.util.Vector;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.jerrywu.database.DBProvider;
import com.jerrywu.database.DataContext;
import com.jerrywu.database.IDataContext;
import com.jerrywu.database.DatabaseTab.*;
import com.jerrywu.log.MessageFactory;
import com.jerrywu.plugin.aws.dynamodb.typeconvertes.ITypeConverter;
import com.jerrywu.plugin.aws.dynamodb.typeconvertes.TypeConverterFactory;
import com.amazonaws.*;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;



public class AWSDynamoDbProvider extends DBProvider {
	//資料庫連結物件
	public AmazonDynamoDBClient dbConnection ;


	
	

	
	
	

	
	

	
	
	
	
	
	
	
	
	
	
	

	
	
	
	
	
	

	
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
		
	

	
	
	
	
	
	
	

	
	
	
	
	
	
	
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	@Override
	public <T extends DataContext> Vector<T> executionQuerySQL(Class<T> c,
			String sqlCommand, boolean isEasy) throws IOException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> executionQuerySQL(Class<T> c,
			String sqlCommand) throws IOException, InstantiationException,
			IllegalAccessException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			int size, boolean isEasy) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c, int size)
			throws InstantiationException, IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c)
			throws InstantiationException, IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			boolean isEasy) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			String condition, int size, boolean isEasy)
			throws InstantiationException, IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			String condition, int size) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			String condition) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> Vector<T> excutionQuery(Class<T> c,
			String condition, boolean isEasy) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends DataContext> T getDataById(Class<T> c, String id,
			boolean isEasy) throws InstantiationException,
			IllegalAccessException, IOException {
		// TODO Auto-generated method stub
		return null;
	}



	

	

	
	
	
	




	
	
	
	
	
	//***************************************************************************************
	//***************************************************************************************
	//
	//    使用者操作函數
	//
	//***************************************************************************************
	//***************************************************************************************	
	
	/**
	 * 建立Table
	 * @param tableName
	 * @param primaryKey
	 * @throws Exception
	 */
	@Override
	public void createTable(String tableName,String primaryKey) throws Exception 
	{
		this.createTable(tableName, primaryKey,10L,5L);
	}
	
	/**
	 * 建立Table
	 * @param tableName
	 * @param primaryKey
	 * @param readUnits
	 * @param writeUnits
	 * @throws Exception
	 */
	public void createTable(String tableName,String primaryKey,Long readUnits,Long writeUnits) throws Exception 
	{
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
                .withKeySchema(new KeySchemaElement().withAttributeName(primaryKey).withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName(primaryKey).withAttributeType(ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readUnits).withWriteCapacityUnits(writeUnits));
        
        this.dbConnection.createTable(createTableRequest).getTableDescription();
        waitForTableToBecomeAvailable(tableName);
	}
	
	/**
	 * 將物件新增近資料庫
	 * @param awsData
	 * @return
	 * @throws Exception
	 */
	@Override
	public void makePersistent(DataContext awsData) throws Exception {
			
		//取得識別Item
		Map<String, AttributeValue> item = AWSDynamoDbProvider.toItem(awsData);
       
		//執行底層寫入
		this.pushItem(awsData.getClass(), item);
		
	}
		
	/**
	 * 將物件從資料庫移除
	 * @param awsData
	 * @throws Exception
	 */
	@Override
	public void deletePersistent(DataContext awsData) throws Exception 
	{
		//取得識別Item
		Map<String, AttributeValue> keyItem = AWSDynamoDbProvider.toMajorItem(awsData);	
		
		//執行底層刪除
		this.deleteItem(awsData.getClass(), keyItem);	
	}
	
	/**
	 * 將物件從資料庫移除
	 * @param c
	 * @param key
	 * @throws Exception
	 */
	public <T extends DataContext> void deletePersistent(Class<T> c , Object key) throws Exception
	{
		//取得識別Item
		Map<String, AttributeValue> keyItem = AWSDynamoDbProvider.toMajorItem(c, key);	
		
		//建立刪除Item
		keyItem.put(c.newInstance().getItemName(), TypeConverterFactory.GetConvertType(c).objectToAttributeValue(key));
		
		//執行底層刪除
		this.deleteItem(c, keyItem);
	}
	
	/**
	 * 利用ＰＫ取得GetItemResult
	 * @param c
	 * @param pk
	 * @return
	 * @throws Exception
	 */
	public <T extends DataContext> GetItemResult getGetItemResultById(Class<T> c, String key) throws Exception
	{
		//建立查詢參數
		Map<String, AttributeValue> getKey = AWSDynamoDbProvider.toMajorItem(c, key);
		//對資料庫進行查詢
		GetItemResult result = this.getGetItemResult(c, getKey);
		return result;
	}
	
	 /**
	 * 利用ＰＫ取得物件
	 */
	@Override
	public <T extends DataContext> T getDataById(Class<T> c, String id) throws InstantiationException, IllegalAccessException, IOException {

		
		T obj = null;
		try 
		{

			GetItemResult result = this.getGetItemResultById(c,id);

			
			
			Map<String, AttributeValue> item = null;
			
			if(result != null)
			{
				item = result.getItem();
			}
			
			
			if( item!= null)
			{
				
				obj = AWSDynamoDbProvider.toObject(c, item);
			}
			else
			{
				MessageFactory.sql().print("查無資料");
			}
			
		} catch (Exception e) {
			MessageFactory.system().print(e.toString());
		}
		
		return obj;
	}
	
	/**
	 * 利用ScanRequest 來取得查詢結果的物件陣列
	 * 最後修正時間2014:03:15
	 * @param c
	 * @param scanRequest
	 * @return
	 * @throws Exception
	 */
	public <T extends DataContext> Vector<T> getData(Class<T> c, ScanRequest scanRequest) throws Exception
	{
		//建立查詢參數
		//ScanResult result = this.getScanResult(scanRequest);
		
		
		
		
		
		Vector<T> item = new Vector<T>();
		try 
		{			
		    ScanResult result = null;
		    do{
			    result = this.getScanResult(scanRequest); 
				for (Map<String, AttributeValue> temp : result.getItems()) {
	                T obj = AWSDynamoDbProvider.toObject(c, temp);
	                item.add(obj);
	            }
			    scanRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
		    }while(result.getLastEvaluatedKey() != null);
		}
		catch(Exception e)
		{
			MessageFactory.error().print(e.toString());
		}
		return item;
	}
	
	/**
	 * 利用QueryRequest 來取得查詢結果的物件陣列
	 * @param c
	 * @param queryRequest
	 * @return
	 * @throws Exception
	 */
	public <T extends DataContext> Vector<T> getData(Class<T> c, QueryRequest queryRequest) throws Exception
	{
		//建立查詢參數
		QueryResult result = this.getQueryResult(queryRequest);
		Vector<T> item = new Vector<T>();
		try 
		{			
           for (Map<String, AttributeValue> temp : result.getItems()) {
                T obj = AWSDynamoDbProvider.toObject(c, temp);
                item.add(obj);
            }

		}
		catch(Exception e)
		{
			MessageFactory.error().print(e.toString());
		}
		return item;
	}	
	
	/**
	 * 利用條件查詢出物件陣列
	 * @param c
	 * @param conditions
	 * @return
	 * @throws Exception
	 */
	public <T extends DataContext> Vector<T> getDataByScanConditions(Class<T> c, Map<String, Condition> conditions) throws Exception
	{
		 ScanRequest scanRequest = AWSDynamoDbProvider.scan(c).withScanFilter(conditions);
		 
		 
		 Vector<T> v = this.getData(c, scanRequest);
		 
		 return v;
	}
	
	/**
	 * 利用條件查詢出物件陣列
	 * @param c
	 * @param conditions
	 * @return
	 * @throws Exception
	 */
	public <T extends DataContext> Vector<T> getDataByQueryConditions(Class<T> c, Map<String, Condition> conditions) throws Exception
	{
		 QueryRequest queryRequest = AWSDynamoDbProvider.query(c).withKeyConditions(conditions);
		 
		 Vector<T> v = this.getData(c, queryRequest);
		 
		 return v;
	}
	
	//***************************************************************************************
	//***************************************************************************************
	//
	//    AWS 底層操作函數 （實際連接資料庫）
	//
	//***************************************************************************************
	//***************************************************************************************
	
	/**
	 * 利用 key值取得 GetItemResult
	 * @param c
	 * @param key
	 * @return
	 * @throws Exception
	 */
	private <T extends DataContext> GetItemResult  getGetItemResult(Class<T> c , Map<String, AttributeValue> key) throws Exception
	{
		GetItemRequest getItemRequest = new GetItemRequest()
	    .withTableName(c.newInstance().getTableName())
	    .withKey(key);
		
		if(this.dbConnection == null)
		{
			MessageFactory.sql().print("資料庫連線中斷或未開啓");
			return null;
		}
		
		GetItemResult result = this.dbConnection.getItem(getItemRequest);
		
		return result;
	}
	
	/**
	 * 利用 queryRequest 值取得 QueryResult
	 * @param c
	 * @param queryRequest
	 * @return
	 * @throws Exception
	 */
	private QueryResult getQueryResult(QueryRequest queryRequest) throws Exception
	{
		QueryResult result = null;
		try 
		{
			if(this.dbConnection == null)
			{
				MessageFactory.sql().print("資料庫連線中斷或未開啓");
				return null;
			}
			
			//取得查詢結果
			result = this.dbConnection.query(queryRequest);
		}
		catch(Exception e)
		{
			MessageFactory.error().print(e.toString());
		}
		
		return result;
	}
	
	
	
	
	/**
	 * 利用ScanRequest 取得 ScanResult
	 * @param scanRequest
	 * @return
	 * @throws Exception
	 */
	private ScanResult getScanResult(ScanRequest scanRequest) throws Exception
	{
		if(this.dbConnection == null)
		{
			MessageFactory.sql().print("資料庫連線中斷或未開啓");
			return null;
		}
		
		
		ScanResult result = null;
		try
		{
			//取得查詢結果
			result = this.dbConnection.scan(scanRequest);
		}
		catch(Exception e)
		{
			MessageFactory.error().print(e.toString());
		}
		
		return result;
	}
	
	/**
	 * 利用ScanRequest 取得 ScanResult
	 * @param scanRequest
	 * @return
	 * @throws Exception
	 */
	private ScanResult getScanResult(ScanRequest scanRequest,Map<String,AttributeValue> startKey) throws Exception
	{
		if(this.dbConnection == null)
		{
			MessageFactory.sql().print("資料庫連線中斷或未開啓");
			return null;
		}
		
		
		ScanResult result = null;
		try
		{
			//取得查詢結果
			scanRequest.setExclusiveStartKey(result.getLastEvaluatedKey());
			result = this.dbConnection.scan(scanRequest);
		}
		catch(Exception e)
		{
			MessageFactory.error().print(e.toString());
		}
		
		return result;
	}
	
	
	
	
	/**
	 * 將Item 新增進資料庫
	 * @param item
	 * @return
	 * @throws  
	 * @throws Exception 
	 */
	private <T extends DataContext> PutItemResult pushItem(Class<T> c , Map<String, AttributeValue> item) throws Exception
	{
		if(this.dbConnection == null)
		{
			MessageFactory.sql().print("資料庫連線中斷或未開啓");
			return null;
		}
		
		
		PutItemRequest putItemRequest = new PutItemRequest()
		  .withTableName(c.newInstance().getTableName())
		  .withItem(item);
		
		PutItemResult result = this.dbConnection.putItem(putItemRequest);
		return result;
	}
	
	/**
	 * 將Item 從資料庫移除
	 * @param item
	 * @return
	 * @throws  
	 * @throws Exception 
	 */
	private <T extends DataContext> DeleteItemResult deleteItem(Class<T> c , Map<String, AttributeValue> keyItem) throws Exception
	{
		if(this.dbConnection == null)
		{
			MessageFactory.sql().print("資料庫連線中斷或未開啓");
			return null;
		}
		
		
		DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
	    .withTableName(c.newInstance().getTableName())
	    .withKey(keyItem);
		
		DeleteItemResult deleteItemResult = this.dbConnection.deleteItem(deleteItemRequest);
		
		return deleteItemResult;
	}
	
	
	//***************************************************************************************
	//***************************************************************************************
	//
	//    資料庫基本控制函數
	//
	//***************************************************************************************
	//***************************************************************************************	
	
	/**
	 * 建立資料庫連線
	 */
	@Override
	public void open() {
		this.dbConnection = BaseAWSDynamoDB.getConnection();
	}

	/**
	 * 關閉資料庫連線
	 */
	@Override
	public void close() {
		AmazonDynamoDBClient temp = this.dbConnection;
		this.dbConnection = null;
		BaseAWSDynamoDB.pushConnection(temp);
	}
	
	/**
	 * 取得查詢物件
	 * @return
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static <T extends DataContext>  QueryRequest query(Class<T> c) throws Exception
	{
		DataContext dc = c.newInstance();
		QueryRequest queryRequest = new QueryRequest().withTableName(dc.getTableName());
		return queryRequest;
	}
	
	/**
	 * 取得掃描物件
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public static <T extends DataContext>  ScanRequest scan(Class<T> c) throws Exception
	{
		DataContext dc = c.newInstance();
		ScanRequest scanRequest = new ScanRequest().withTableName(dc.getTableName());
		return scanRequest;
	}

	
	//***************************************************************************************
	//***************************************************************************************
	//
	//    形態轉換函數
	//
	//***************************************************************************************
	//***************************************************************************************
	
	/**
	 * 將Object轉換成Item
	 * @param awsData
	 * @return
	 * @throws Exception
	 */
	public static Map<String, AttributeValue> toItem(IDataContext awsData) throws Exception
	{
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
		for (Field field : awsData.getClass().getDeclaredFields()) 
		{	
			field.setAccessible(true);
			//判斷是否需要實體化
			if(!field.isAnnotationPresent(NonPersistent.class) && !field.isAnnotationPresent(ReferKey.class) )
			{
				// 取得欄位名稱(如果有標籤的話以標籤為主)
				String fieldName = field.getName();
				if (field.isAnnotationPresent(DBFieldName.class)) {
					fieldName = field.getAnnotation(DBFieldName.class).value();
				}

				if (field.isAnnotationPresent(PrimaryKey.class)) {
					fieldName = field.getAnnotation(PrimaryKey.class).value();
				}
				
				
				// 取得欄位數值
				AttributeValue fieldValue = null;
				if (field.get(awsData) != null ) {
					
					//形態轉換
					ITypeConverter converter = null;
					
					if(field.getType() == Vector.class)
					{
					
						converter = TypeConverterFactory.GetConvertType(field.getType(),String.class);
					}
					else
					{
						converter = TypeConverterFactory.GetConvertType(field.getType());
					}
					
					fieldValue = converter.objectToAttributeValue(field.get(awsData));
				}
				
				//新增進入item
				item.put(fieldName, fieldValue);
			}
		}
		
		return item;
	}
	
	/**
	 * 利用 Object 取得 MajorItem
	 * @param awsData
	 * @return
	 * @throws Exception
	 */
	public static Map<String, AttributeValue> toMajorItem (IDataContext awsData)  throws Exception
	{
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put(awsData.getItemName(), TypeConverterFactory.GetConvertType(String.class).objectToAttributeValue(awsData.getItemValue()));
		return item;
	}

	/**
	 * 利用 key 取得 MajorItem
	 * @param c
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public static <T extends DataContext> Map<String, AttributeValue> toMajorItem (Class<T> c, Object key)  throws Exception
	{
		T obj = c.newInstance();
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put(obj.getItemName(), TypeConverterFactory.GetConvertType(String.class).objectToAttributeValue(key));
		return item;
	}	

	/**
	 * 將item轉為object
	 * @param c
	 * @param attributeValue
	 * @return
	 * @throws Exception
	 */
	public static <T extends DataContext> T toObject(Class<T> c, Map<String, AttributeValue> attributeValue) throws Exception
	{
		
		//建立物件
		T obj = c.newInstance();
		
		//讀取所有元素
		for (Map.Entry<String, AttributeValue> item : attributeValue.entrySet()) 
		{
			
			  	// 欄位名稱
			  	String attributeName = item.getKey();
			  				  	
			  	//掃瞄物件
				for (Field field : obj.getClass().getDeclaredFields()) {
						field.setAccessible(true);
						
						//判斷是否需要實體化
						if(!field.isAnnotationPresent(NonPersistent.class) && !field.isAnnotationPresent(ReferKey.class) )
						{
							// 取得欄位名稱(如果有標籤的話以標籤為主)
							String fieldName = field.getName();
							if (field.isAnnotationPresent(DBFieldName.class)) {
								fieldName = field.getAnnotation(DBFieldName.class)
										.value();
							}
							
							if (field.isAnnotationPresent(PrimaryKey.class)) {
								fieldName = field.getAnnotation(PrimaryKey.class).value();
							}
							
							// 物件與item的mapping
							if (attributeName.equals(fieldName)) {
								ITypeConverter converter = null;
								if(field.getType() == Vector.class)
								{

									converter = TypeConverterFactory.GetConvertType(field.getType(),String.class);
								}
								else
								{
									converter = TypeConverterFactory.GetConvertType(field.getType());
								}
							
								field.set(obj, converter.attributeValueToObject(item.getValue()));
							}
							
						}
				}
   
		}
		return obj;
	}
	
	/**
	 * 建立表格函數
	 * @param tableName
	 */
    private void waitForTableToBecomeAvailable(String tableName) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {Thread.sleep(1000 * 20);} catch (Exception e) {}
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
                TableDescription tableDescription = this.dbConnection.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase("ResourceNotFoundException") == false) throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }

}

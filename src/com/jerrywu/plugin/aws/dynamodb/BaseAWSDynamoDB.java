package com.jerrywu.plugin.aws.dynamodb;


import java.util.Vector;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 * AWS基本連線類別
 * @author Jerry
 *
 */
public class BaseAWSDynamoDB {
	
	//禁止此類別初始化
	private BaseAWSDynamoDB(){};

	//connection pool
	private static Vector<AmazonDynamoDBClient> vConnection;
		
	//pushConnection
	public static void pushConnection(AmazonDynamoDBClient connection)
	{
		if(vConnection == null)
		{
			vConnection = new Vector<AmazonDynamoDBClient>();
		}
		
		vConnection.add(connection);
	}

	//取得資料庫連線
	public static AmazonDynamoDBClient getConnection(Regions rs)
	{
		if(vConnection == null || vConnection.size() == 0)
		{
			return createConnection(rs);
		}
		else
		{
			AmazonDynamoDBClient dbConnection = vConnection.get(0);
			vConnection.remove(0);
			return dbConnection;
		}
	}
	
	
	//取得資料庫連線(預設新加坡)
	public static AmazonDynamoDBClient getConnection()
	{
		return getConnection(Regions.AP_SOUTHEAST_1);
	}
	
	
	//建立資料庫連線(地區)
	private static AmazonDynamoDBClient createConnection(Regions rs)
	{
		AmazonDynamoDBClient dbConnection = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
		Region region = Region.getRegion(rs);
		dbConnection.setRegion(region);
		return dbConnection;
	}
	
	
}

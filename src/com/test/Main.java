package com.test;

import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;


public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("Hello World");
		
		//建立連接通道
		AmazonDynamoDBClient client;
		AWSCredentials credentials;
		
		try {
			credentials = new PropertiesCredentials(
			        Main.class.getResourceAsStream("AwsCredentials.properties"));
			
			client = new AmazonDynamoDBClient(credentials);
		       
		} catch (Exception e) {
			System.out.println("Hello World");
		}

		
		
	}

}

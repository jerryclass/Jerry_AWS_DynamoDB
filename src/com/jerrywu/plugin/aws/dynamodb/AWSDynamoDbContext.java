package com.jerrywu.plugin.aws.dynamodb;

import java.lang.reflect.Field;

import org.json.*;
import com.jerrywu.database.DataContext;
import com.jerrywu.database.DatabaseTab.*;


public class AWSDynamoDbContext extends DataContext{


	
	/**
	 * 取得表格名稱
	 */
	@Override
	public String getTableName() {
		String tableName = this.getClass().getSimpleName();
		//判斷使否有使用標籤，如果有用標籤來取代類別名稱來當作資料表名稱
		if (this.getClass().isAnnotationPresent(TableName.class)) {
			tableName = this.getClass().getAnnotation(TableName.class).value();
		}
		return tableName;
	}


	@Override
	/**
	 * 取得欄位PK
	 * @return PK 名稱
	 */
	public String getItemName() {
		String itemName = "ID";
		//判斷使否有使用標籤，如果有用標籤來取代類別名稱來當作資料表名稱
		for (Field field : this.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			if (field.isAnnotationPresent(PrimaryKey.class)) {
				itemName = field.getAnnotation(PrimaryKey.class).value();
			}
		}
		return itemName;
	}

	@Override
	/**
	 * 取得欄位PK Value
	 * @return PK Value
	 */
	public String getItemValue() {
		String value = "";
		for (Field field : this.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			if (field.isAnnotationPresent(PrimaryKey.class)) {
				try {
					value = String.valueOf(field.get(this));
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
		return value;
	}
	
	
	/**
	 * 取得FK欄位
	 * @param <T>
	 * @param type 參照欄位的型態
	 * @return 欄位
	 */
	@Override
	public <T> Field  getForeignKeyField(Class <T> type)
	{
		for (Field field : this.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			//查詢FK欄位
			if(field.isAnnotationPresent(ForeignKey.class) &&  field.getAnnotation(ForeignKey.class).value() ==  type)
			{
					return field;
			}
		}
		return null;
	}	
	

	
	
	@Override
	public JSONObject toJSONObject() throws Exception {

		
		StringBuilder jo = new StringBuilder();
		jo.append("{");
		jo.append(String.format("%s=\"%s\"", "itemname",this.getItemName()));
		
		for (Field field : this.getClass().getDeclaredFields()) {
			jo.append(",");
			
			field.setAccessible(true);
			// 取得欄位輸出名稱(如果有標籤的話以標籤為主)
			String fieldName = field.getName();
			if (field.isAnnotationPresent(OutputFieldName.class)) {
				fieldName = field.getAnnotation(OutputFieldName.class)
						.value();
			}
			
			if( !field.isAnnotationPresent(ReferKey.class))
			 {//非關聯JSON
						
						if(field.get(this) != null && field.get(this).getClass() == String.class)
						{
							jo.append(String.format("%s=\"%s\"", fieldName, field.get(this).toString()));
						}
						else if(field.get(this) != null)
						{
							jo.append(String.format("%s=%s", fieldName, field.get(this).toString()));
						}
						
			 }
			 else
			 {//關聯JSON
				 	   
				 		
			 }
			
		}
		
		jo.append("}");
		JSONObject result = new JSONObject(jo.toString());
		return result;
		
	}







}

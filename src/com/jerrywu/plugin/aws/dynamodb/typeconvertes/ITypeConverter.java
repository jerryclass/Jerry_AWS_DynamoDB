package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.*;

/**
 * 轉換介面
 */
public interface ITypeConverter {
		//型態轉換
		public Object attributeValueToObject(AttributeValue value);
		
		public AttributeValue objectToAttributeValue(Object value);
}

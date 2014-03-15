package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * 
 * 轉成字串
 *
 */
public class StringConverter implements ITypeConverter{

	@Override
	public Object attributeValueToObject(AttributeValue value) {
			if(value == null)
			{
				 return "";
			}
		
			return value.getS();
	}

	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		String sValue = String.valueOf(value);
		return (new AttributeValue().withS(sValue));
	}


}

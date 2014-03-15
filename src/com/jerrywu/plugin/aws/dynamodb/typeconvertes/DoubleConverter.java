package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * 字串轉Double
 *
 */
public class DoubleConverter implements ITypeConverter{

	@Override
	public Object attributeValueToObject(AttributeValue value) {
		if(value == null || value.equals("") )
		{
			 return Double.parseDouble("0.0");
		}
		else
		{
			 return Double.parseDouble(value.getN());
		}
	}

	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		String sValue = String.valueOf(value);
		return (new AttributeValue().withN(sValue));
	}
	
}

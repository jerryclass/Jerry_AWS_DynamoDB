package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * 字串轉整數
 *
 */
public class IntegerConverter implements ITypeConverter {

	@Override
	public Object attributeValueToObject(AttributeValue value) {
		if(value == null)
		{
			 return 0;
		}
		else
		{
			return Integer.parseInt(value.getN());
		}

	}

	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		String sValue = String.valueOf(value);
		return (new AttributeValue().withN(sValue));
	}

}

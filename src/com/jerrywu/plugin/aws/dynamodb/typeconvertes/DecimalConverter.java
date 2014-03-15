package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;


/**
 * 十六進為轉十進位
 *
 */
public class DecimalConverter implements ITypeConverter {

	@Override
	public Object attributeValueToObject(AttributeValue value) {
		if(value == null || value.getS().equals(""))
		{
			 return 0;
		}
		else
		{
			Integer.parseInt(value.getS(), 16);
		}
		return null;
	}

	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		// TODO Auto-generated method stub
		return null;
	}

}

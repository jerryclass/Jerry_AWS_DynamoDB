package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import java.util.Vector;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class VectorSConverter implements ITypeConverter{

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object attributeValueToObject(AttributeValue value) {
		
		if(value == null)
		{
			 return null;	 
		}

		Vector<String> result = new Vector(value.getSS());
		return result;			
	}


	@SuppressWarnings("unchecked")
	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		Vector<String> v = (Vector<String>)value;
		return (new AttributeValue().withSS(v));	
	}
}

package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import java.util.Vector;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class CopyOfVectorSConverter implements ITypeConverter{

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object attributeValueToObject(AttributeValue value) {
		
		if(value == null)
		{
			 return null;	 
		}

		Vector<String> result = new Vector(value.getSS());
		return result;	
		
		
		
		/*
		if(value.getSS() != null)
		{

		}
		else
		{
			Vector<String> v = new Vector(value.getNS());		
			
			Vector<Integer> rv = new Vector<Integer>();
			
			for(String temp : v)
			{
				rv.add(Integer.valueOf(temp));
			}
			
			return rv;
		}
		*/
		
	}

	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		Vector tempType = (Vector)value;
		if(tempType.firstElement()!=null)
		{
			Vector<String> v = (Vector<String>)tempType;
			return (new AttributeValue().withSS(v));
			
			/*
			if(tempType.firstElement().getClass() == String.class)
			{//字串
				Vector<String> v = (Vector<String>)tempType;
				return (new AttributeValue().withSS(v));
			}
			else
			{
				Vector<String> v = new Vector<String>();	
				for(Object tv : (Vector)tempType)
				{
					v.add(String.valueOf(tv));
				}
				return (new AttributeValue().withNS(v));
			}
			*/
		}
		else
		{
			return null;
		}		
	}
}

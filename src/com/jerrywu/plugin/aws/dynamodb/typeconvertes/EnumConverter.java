package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;



public class EnumConverter implements ITypeConverter{

	@Override
	public Object attributeValueToObject(AttributeValue value) {
		//列舉轉換不適用，因此丟出一個例外訊息 
		//throw new NotImplementedException();
		return null;
	}
	
    @SuppressWarnings("unchecked")
	public  Object attributeValueToObject(@SuppressWarnings("rawtypes") Class EnumType, String value)
    {
    	if(!EnumType.isEnum())
    	{
    		 //throw new NotImplementedException();
    		return null;
    	}
    	else
    	{
    		 return Enum.valueOf(EnumType, value);
    	}
    	/*
        if (!EnumType.IsEnum)
            throw new InvalidOperationException("ERROR_TYPE_IS_NOT_ENUMERATION");
        return System.Convert.ChangeType(Enum.Parse(EnumType, ValueToConvert.ToString()), EnumType);
        */

    }

	@Override
	public AttributeValue objectToAttributeValue(Object value) {
		// TODO Auto-generated method stub
		return null;
	}

}

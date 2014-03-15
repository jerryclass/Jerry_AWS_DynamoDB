package com.jerrywu.plugin.aws.dynamodb.typeconvertes;

import java.util.Vector;


/**
 * 
 * 轉換工廠，決定要實現哪個轉換類別-簡單工廠模式
 *
 */
public class TypeConverterFactory {
	
	
		public static  ITypeConverter GetConvertType(Class<?> type , Class<?> subType)
		{
			if(type == Vector.class || type.getSimpleName().toString().equals("vector"))
			{
			       if(subType == String.class)
				   {//字串
			    	   return (new VectorSConverter());
				   }  
			}
			return null;
		}
	
		public static  ITypeConverter GetConvertType(Class<?> type){
		       if(type == String.class)
			   {//字串
		    	   return (new StringConverter());
			   }
			   else if(type == Integer.class || type.toString().equals("int"))
			   {//整數
				   return (new IntegerConverter());
			   }
			   else if(type == Double.class || type.toString().equals("double"))
			   {//浮點數
				   return (new DoubleConverter());
			   }
			   else if(type == Long.class || type.toString().equals("long"))
			   {//長整數
				   return (new LongConverter());
			   }
			   else if(type.isEnum())
			   {//列舉
			       return (new EnumConverter());
			   }
			   else
			   {
				   return null;
			   }
			
		}
}

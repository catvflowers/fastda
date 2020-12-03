package com.sm.fastdata.web;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import com.alibaba.fastjson.serializer.ValueFilter;

public class JSONSerializeFilter implements ValueFilter  {
	private SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat  dtf2 = new SimpleDateFormat("yyyy-MM-dd");
	@Override
	public Object process(Object object, String name, Object value) {
		// TODO Auto-generated method stub
		if(value instanceof Date) {
			long timestamp =((Date)value).getTime();
			String str=dtf2.format(new java.util.Date(timestamp));
			return str;
		}else if(value instanceof Timestamp) {
			long timestamp =((Timestamp)value).getTime();
			String str=dtf.format(new java.util.Date(timestamp));
			return str;
		}
		return value;
	}

}

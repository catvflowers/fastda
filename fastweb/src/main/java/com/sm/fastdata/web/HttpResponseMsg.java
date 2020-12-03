package com.sm.fastdata.web;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import com.sm.fastda.FastDAContext;

public class HttpResponseMsg {
	public static void message(HttpServletResponse response,FastDAContext context) {
		boolean status=context.status;
		String code="requestId="+String.valueOf(context.requestId)+";text="+context.errorMessage;
		String message="";
		if(status) message="执行成功";
		else message="执行失败";
		int total=context.total;
		String str=null;
		try {
			str = new String(message.getBytes("UTF-8"),"ISO8859-1");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		response.setHeader("x-message", str);
		response.setHeader("x-code", code);
		response.setHeader("x-status", String.valueOf(status));
		if(total>-1)
			response.setHeader("x-total-count", String.valueOf(total));
	}
}

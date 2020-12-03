package com.sm.fastdata.web;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeFilter;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.sm.fastda.FastDAContext;
import com.sm.fastda.FastDAExtecutor;

@Component
public class FastControllerImpl<T> implements IFastController  {
	private static Logger logger = LoggerFactory.getLogger(FastControllerImpl.class);
	private  Class<T> clz;
	private FastDAExtecutor extecutor;
	private List<Map<String, String>> urlList;
	
	private final String port = getUrlValue("custom.port");
	private final SerializeFilter[] sf=new SerializeFilter[] {new JSONSerializeFilter()};
	public void setUrlList(List<Map<String, String>> urlList){
		this.urlList = urlList;
	}
	
	public void setExtecutor(FastDAExtecutor extecutor) {
		this.extecutor = extecutor;
	}
	
	public void setClz(Class<T> clz) {
		this.clz = clz;
	}

	private Map<String,String> getParams(HttpServletRequest resquest){
		Map<String,String> params=new HashMap<>();
		Enumeration<String> keys=  resquest.getParameterNames();
		while(keys.hasMoreElements()) {
			String key=keys.nextElement();
			params.put(key, resquest.getParameter(key));
		}
		return params;
	}
	
	private String getUrlValue(String urlName) {
        String url = null;
        Properties prop = new Properties();
        try {
            //InputStream in = new BufferedInputStream(new FileInputStream("./custom.properties"));
            //prop.load(in); /// 加载属性列表
        	ClassLoader classLoader = FastControllerImpl.class.getClassLoader();
        	InputStream in = classLoader.getResourceAsStream("custom.properties");
        	prop.load(in);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while (it.hasNext()) {
                if (it.next().equals(urlName)) {
                    url = prop.getProperty(urlName);
                }
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }
	
	private String getRequestBody(InputStream stream) throws IOException {
		String line = null;
		StringBuilder body = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		while ((line = reader.readLine()) != null) {
			body.append(line);
		}
		reader.close();
		return body.toString();
	}
	
	private void out(Object result,HttpServletResponse response) {
		String str=JSON.toJSONString(result,sf,SerializerFeature.DisableCircularReferenceDetect);
		try {
			response.setContentType("application/json;charset=UTF-8");
			response.getWriter().write(str);
			response.getWriter().close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	private void getRequestHeader(HttpServletRequest resquest,FastDAContext context) {
		String requestId=resquest.getHeader("requestId");
		if(requestId!=null) context.requestId=Integer.parseInt(requestId);
	}
	private void printLogs(FastDAContext context) {
		String temp=context.getLogs();
		String[] arr=temp.split(";;");
		int len=arr.length/2;
		if(len==0) return;
		for(int i=0;i<len;i=i+2) {
			if(arr[i].equals("0"))
				logger.info(arr[i+1]);
			else logger.error(arr[i+1]);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void sendRequest(String url,String type,String params){
		RestTemplate client = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        HttpMethod method = null;
        switch(type){
        	case "GET":
        		method = HttpMethod.GET;
        		break;
        	case "POST":
        		method = HttpMethod.POST;
        		break;
        	case "PUT":
        		method = HttpMethod.PUT;
        		break;
        	case "PATCH":
        		method = HttpMethod.PATCH;
        		break;
        	case "DELETE":
        		method = HttpMethod.DELETE;
        		break;
        	default:
        		return;
        }
        // 以表单的方式提交
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        //将请求头部和参数合成一个请求
        HttpEntity requestEntity = new HttpEntity(params, headers);
        System.out.println("url= "+url);
        System.out.println("请求体为:"+requestEntity.getBody());
		client.exchange(url, method, requestEntity, String.class);
		System.out.println("请求结束");
	}
	
	/**
	 * 从请求信息中获取请求的IP及端口号
	 * */
	private String getLocalPort(HttpServletRequest resquest) {
		String addr = resquest.getLocalAddr();
		addr="127.0.0.1";
		//int port = resquest.getLocalPort();
		return "http://"+addr+":"+port+"/";
	}
	
	@Override
	@ResponseBody
	public void queryAll(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("queryAll url="+url);
		Map<String,String> params=getParams(resquest);
		FastDAContext context=FastDAContext.getValue();
		getRequestHeader(resquest,context);
		List<?> result=extecutor.queryForList(clz, params);
		//Map<String,Object> context=new HashMap<>();
		String resultJson = JSON.toJSONString(result);
		if(urlList!=null && urlList.size()>0){
			for (Map<String,String> map : urlList) {
				if(map.get("type").equals("GET")){
					sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
					//resultJson = JSONObject.toJSONString(entity.getBody());
				}else{
					continue;
				}
			}
		}
		printLogs(context);
		HttpResponseMsg.message(response, context);
		out(result,response);
	}

	@Override
	@ResponseBody
	public void insertAll(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("insertAll url="+url);
		InputStream stream;
		try {
			stream = resquest.getInputStream();
			//String body=getRequestBody(resquest);
			String body=getRequestBody(stream);
			System.out.println("body="+body);
			stream.close();
			if(body==null) return;
			List<T>  list=null;
			if(body.startsWith("{")) {
				T obj=JSON.parseObject(body,this.clz);
				list=new ArrayList<>(1);
				list.add(obj);
			}else if(body.startsWith("[")) {
				list=JSON.parseArray(body, this.clz);
			}
			FastDAContext context=FastDAContext.getValue();
			getRequestHeader(resquest,context);
			System.out.println("新增之前："+JSON.toJSONString(list));
			extecutor.insertBatchObject(list, this.clz);
			String resultJson = JSON.toJSONString(list);
			System.out.println("新增之后:"+resultJson);
			if(urlList!=null && urlList.size()>0){
				for (Map<String,String> map : urlList) {
					if(map.get("type").equals("POST")){
						sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
						//resultJson = JSONObject.toJSONString(entity.getBody());
					}else{
						continue;
					}
				}
			}
			printLogs(context);
			HttpResponseMsg.message(response, context);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void updateAll(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("updateAll url="+url);
		InputStream stream;
		try {
			stream = resquest.getInputStream();
			String body=getRequestBody(stream);
			stream.close();
			if(body==null) return;
			List<T>  list=null;
			if(body.startsWith("{")) {
				T obj=JSON.parseObject(body,this.clz);
				list=new ArrayList<>(1);
				list.add(obj);
			}else if(body.startsWith("[")) {
				list=JSON.parseArray(body, this.clz);
			}
			FastDAContext context=FastDAContext.getValue();
			getRequestHeader(resquest,context);
			extecutor.updateBatchObject(list, this.clz);
			String resultJson = JSON.toJSONString(list);
			if(urlList!=null && urlList.size()>0){
				for (Map<String,String> map : urlList) {
					if(map.get("type").equals("PUT") || map.get("type").equals("PATCH")){
						sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
						//resultJson = JSONObject.toJSONString(entity.getBody());
					}else{
						continue;
					}
				}
			}
			printLogs(context);
			HttpResponseMsg.message(response, context);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void deleteAll(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("deleteAll url="+url);
		InputStream stream;
		try {
			stream = resquest.getInputStream();
			String body=getRequestBody(stream);
			stream.close();
			if(body==null) return;
			List<T>  list=null;
			if(body.startsWith("{")) {
				T obj=JSON.parseObject(body,this.clz);
				list=new ArrayList<>(1);
				list.add(obj);
			}else if(body.startsWith("[")) {
				list=JSON.parseArray(body, this.clz);
			}
			FastDAContext context=FastDAContext.getValue();
			getRequestHeader(resquest,context);
			extecutor.deleteBatchObject(list, this.clz);
			String resultJson = JSON.toJSONString(list);
			if(urlList!=null && urlList.size()>0){
				for (Map<String,String> map : urlList) {
					if(map.get("type").equals("DELETE")){
						sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
						//resultJson = JSONObject.toJSONString(entity.getBody());
					}else{
						continue;
					}
				}
			}
			printLogs(context);
			HttpResponseMsg.message(response, context);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void query(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("query url="+url);
		Map<String,String> params=getParams(resquest);
		int pos=url.lastIndexOf("/");
		String idstr=url.substring(pos+1);
		params.put("id", idstr);
		FastDAContext context=FastDAContext.getValue();
		getRequestHeader(resquest,context);
		List<?> result=extecutor.queryForList(clz, params);
		printLogs(context);
		String resultJson = JSON.toJSONString(result);
		if(urlList!=null && urlList.size()>0){
			for (Map<String,String> map : urlList) {
				if(map.get("type").equals("GET")){
					sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
					//resultJson = JSONObject.toJSONString(entity.getBody());
				}else{
					continue;
				}
			}
		}
		HttpResponseMsg.message(response, context);
		if(result!=null&&result.size()>0)
			out(result.get(0),response);
	}

	@Override
	public void update(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("update url="+url);
		InputStream stream;
		try {
			stream = resquest.getInputStream();
			//String body=getRequestBody(resquest);
			String body=getRequestBody(stream);
			stream.close();
			int pos=url.lastIndexOf("/");
			String idstr=url.substring(pos+1);
			if(body==null) return;
			List<T>  list=null;
			if(body.startsWith("{")) {
				T obj=JSON.parseObject(body,this.clz);
				list=new ArrayList<>(1);
				list.add(obj);
				Integer id=new Integer(idstr);
				try {
					obj.getClass().getDeclaredField("id").set(obj, id);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}else return;
			FastDAContext context=FastDAContext.getValue();
			getRequestHeader(resquest,context);
			extecutor.updateBatchObject(list, this.clz);
			String resultJson = JSON.toJSONString(list);
			if(urlList!=null && urlList.size()>0){
				for (Map<String,String> map : urlList) {
					if(map.get("type").equals("PUT") || map.get("type").equals("PATCH")){
						sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
						//resultJson = JSONObject.toJSONString(entity.getBody());
					}else{
						continue;
					}
				}
			}
			printLogs(context);
			HttpResponseMsg.message(response, context);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void delete(HttpServletRequest resquest, HttpServletResponse response) {
		String url=resquest.getRequestURI();
		String localPort = getLocalPort(resquest);
		System.out.println("delete url="+url);
		int pos=url.lastIndexOf("/");
		String str=url.substring(pos+1);
		String body="{\"id\":"+str+"}";
		T obj=JSON.parseObject(body,this.clz);
		List<T> list=new ArrayList<>(1);
		list.add(obj);
		FastDAContext context=FastDAContext.getValue();
		getRequestHeader(resquest,context);
		this.extecutor.deleteBatchObject(list, this.clz);
		String resultJson = JSON.toJSONString(list);
		if(urlList!=null && urlList.size()>0){
			for (Map<String,String> map : urlList) {
				if(map.get("type").equals("DELETE")){
					sendRequest(localPort+map.get("url"), map.get("type"), resultJson);
					//resultJson = JSONObject.toJSONString(entity.getBody());
				}else{
					continue;
				}
			}
		}
		printLogs(context);
		HttpResponseMsg.message(response, context);
	}

}

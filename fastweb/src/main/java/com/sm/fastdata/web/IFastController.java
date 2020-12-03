package com.sm.fastdata.web;

import javax.servlet.http.*;

public interface IFastController {

	public void queryAll(HttpServletRequest resquest,HttpServletResponse response);
	public void insertAll(HttpServletRequest resquest,HttpServletResponse response);
	public void updateAll(HttpServletRequest resquest,HttpServletResponse response);
	public void deleteAll(HttpServletRequest resquest,HttpServletResponse response);
	public void query(HttpServletRequest resquest,HttpServletResponse response);
	public void update(HttpServletRequest resquest,HttpServletResponse response);
	public void delete(HttpServletRequest resquest,HttpServletResponse response);
}

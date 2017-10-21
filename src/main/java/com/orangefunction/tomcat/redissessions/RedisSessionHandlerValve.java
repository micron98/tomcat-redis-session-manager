package com.orangefunction.tomcat.redissessions;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class RedisSessionHandlerValve extends ValveBase {
	private final Log log = LogFactory.getLog(RedisSessionManager.class);
	private RedisSessionManager manager;

	private static ThreadLocal<Request> REQUEST = new ThreadLocal<Request>();

	public static Request getCurrentRequest() {
		return REQUEST.get();
	}

	public void setRedisSessionManager(RedisSessionManager manager) {
		this.manager = manager;
	}

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		REQUEST.set(request);
		try {
			getNext().invoke(request, response);
		} finally {
			REQUEST.remove();
			String q = request.getParameter("hash");
			if (q == null || q.isEmpty()) {
				manager.afterRequest();
			}
		}
	}
}

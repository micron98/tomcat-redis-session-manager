package com.orangefunction.tomcat.redissessions;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Request;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.Pool;

public class RedisSessionManager extends ManagerBase implements Lifecycle, Runnable {

	enum SessionPersistPolicy {
		DEFAULT, SAVE_ON_CHANGE, ALWAYS_SAVE_AFTER_REQUEST;

		static SessionPersistPolicy fromName(String name) {
			for (SessionPersistPolicy policy : SessionPersistPolicy.values()) {
				if (policy.name().equalsIgnoreCase(name)) {
					return policy;
				}
			}
			throw new IllegalArgumentException("Invalid session persist policy [" + name + "]. Must be one of "
					+ Arrays.asList(SessionPersistPolicy.values()) + ".");
		}
	}

	protected byte[] NULL_SESSION = "null".getBytes();

	private final Log log = LogFactory.getLog(RedisSessionManager.class);

	protected String host = "localhost";
	protected int port = 6379;
	protected HostAndPort originalHostAndPort;
	protected HostAndPort defaultHostAndPort;
	protected int database = 0;
	protected String password = null;
	protected int timeout = Protocol.DEFAULT_TIMEOUT;
	protected String sentinelMaster = null;
	protected int maxRetryCount = 5;
	Set<String> sentinelSet = null;

	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

	protected Pool<Jedis> connectionPool;
	protected Map<HostAndPort, Pool<Jedis>> connectionPools;
	protected JedisPoolConfig connectionPoolConfig = new JedisPoolConfig();

	protected RedisSessionHandlerValve handlerValve;
	protected ThreadLocal<RedisSession> currentSession = new ThreadLocal<>();
	protected ThreadLocal<SessionSerializationMetadata> currentSessionSerializationMetadata = new ThreadLocal<>();
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<>();
	protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<>();
	protected Serializer serializer;

	protected Pattern ignorePattern;

	protected static String name = "RedisSessionManager";

	protected String serializationStrategyClass = "com.orangefunction.tomcat.redissessions.JavaSerializer";

	protected EnumSet<SessionPersistPolicy> sessionPersistPoliciesSet = EnumSet.of(SessionPersistPolicy.DEFAULT);

	/**
	 * The lifecycle event support for this component.
	 */
	protected LifecycleSupport lifecycle = new LifecycleSupport(this);

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDatabase() {
		return database;
	}

	public void setDatabase(int database) {
		this.database = database;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setSerializationStrategyClass(String strategy) {
		this.serializationStrategyClass = strategy;
	}

	public String getSessionPersistPolicies() {
		StringBuilder policies = new StringBuilder();
		for (Iterator<SessionPersistPolicy> iter = this.sessionPersistPoliciesSet.iterator(); iter.hasNext();) {
			SessionPersistPolicy policy = iter.next();
			policies.append(policy.name());
			if (iter.hasNext()) {
				policies.append(",");
			}
		}
		return policies.toString();
	}

	public void setSessionPersistPolicies(String sessionPersistPolicies) {
		String[] policyArray = sessionPersistPolicies.split(",");
		EnumSet<SessionPersistPolicy> policySet = EnumSet.of(SessionPersistPolicy.DEFAULT);
		for (String policyName : policyArray) {
			SessionPersistPolicy policy = SessionPersistPolicy.fromName(policyName);
			policySet.add(policy);
		}
		this.sessionPersistPoliciesSet = policySet;
	}

	public boolean getSaveOnChange() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.SAVE_ON_CHANGE);
	}

	public boolean getAlwaysSaveAfterRequest() {
		return this.sessionPersistPoliciesSet.contains(SessionPersistPolicy.ALWAYS_SAVE_AFTER_REQUEST);
	}

	public String getSentinels() {
		StringBuilder sentinels = new StringBuilder();
		for (Iterator<String> iter = this.sentinelSet.iterator(); iter.hasNext();) {
			sentinels.append(iter.next());
			if (iter.hasNext()) {
				sentinels.append(",");
			}
		}
		return sentinels.toString();
	}

	public void setSentinels(String sentinels) {
		if (null == sentinels) {
			sentinels = "";
		}

		String[] sentinelArray = sentinels.split(",");
		this.sentinelSet = new HashSet<String>(Arrays.asList(sentinelArray));
	}

	public Set<String> getSentinelSet() {
		return this.sentinelSet;
	}

	public String getSentinelMaster() {
		return this.sentinelMaster;
	}

	public void setSentinelMaster(String master) {
		this.sentinelMaster = master;
	}

	@Override
	public int getRejectedSessions() {
		// Essentially do nothing.
		return 0;
	}

	public void setRejectedSessions(int i) {
		// Do nothing.
	}

	// protected Jedis acquireConnection() {
	// return acquireConnection(defaultHostAndPort);
	// }
	//
	protected Jedis acquireConnection(HostAndPort host) {
		long elapsed = System.currentTimeMillis();
		Pool<Jedis> connectionPool = connectionPools.get(host);
		if (connectionPool == null) {
			try {
				connectionPool = initializeDatabaseConnection(host);
			} catch (LifecycleException e) {

				ConnectException ce = null;
				JedisConnectionException jce = null;
				Throwable t;
				while ((t = e.getCause()) != null) {
					if (t instanceof ConnectException) {
						ce = (ConnectException) t;
						break;
					} else//
					if (t instanceof JedisConnectionException) {
						jce = (JedisConnectionException) t;
						break;
					}
				}

				if (ce != null || jce != null) {

					// if current host and port is not the original host and port, try the original
					// one. this may be possible when redis advised redirection and later the host
					// become unavailable, we will need to fallback to the original
					if (originalHostAndPort.equals(host) == false) {
						log.warn("connection failed to " + host + ". retrying " + originalHostAndPort);
						return acquireConnection(originalHostAndPort);
					}
				}

				throw new IllegalStateException(e);
			}
		}
		Jedis jedis = connectionPool.getResource();

		if (getDatabase() != 0) {
			jedis.select(getDatabase());
		}

		if (log.isDebugEnabled())
			log.debug("acquireConnection(" + host + ") finished in " + (System.currentTimeMillis() - elapsed) + "ms");

		return jedis;
	}

	protected void retireCurrentPool(HostAndPort hostAndPort) {
		Pool<Jedis> pool = connectionPools.remove(hostAndPort);
		if (pool == null) {
			return;
		}
		try {
			pool.close();
		} catch (Exception e) {
		}
	}

	protected void returnConnection(Jedis jedis, Boolean error) {
		jedis.close();
	}

	protected void returnConnection(Jedis jedis) {
		returnConnection(jedis, false);
	}

	@Override
	public void load() throws ClassNotFoundException, IOException {

	}

	@Override
	public void unload() throws IOException {

	}

	/**
	 * Add a lifecycle event listener to this component.
	 *
	 * @param listener
	 *            The listener to add
	 */
	@Override
	public void addLifecycleListener(LifecycleListener listener) {
		lifecycle.addLifecycleListener(listener);
	}

	/**
	 * Get the lifecycle listeners associated with this lifecycle. If this Lifecycle
	 * has no listeners registered, a zero-length array is returned.
	 */
	@Override
	public LifecycleListener[] findLifecycleListeners() {
		return lifecycle.findLifecycleListeners();
	}

	/**
	 * Remove a lifecycle event listener from this component.
	 *
	 * @param listener
	 *            The listener to remove
	 */
	@Override
	public void removeLifecycleListener(LifecycleListener listener) {
		lifecycle.removeLifecycleListener(listener);
	}

	public void run() {

		if (true)
			return;

		System.out.println("----");

		Jedis jedis = null;
		Boolean error = true;

		long elapsed = System.currentTimeMillis();

		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					//
					ScanParams params = new ScanParams();
					params.match("redis-*");
					ScanResult<String> scanResult = jedis.scan("0", params);
					List<String> keys = scanResult.getResult();
					String nextCursor = scanResult.getStringCursor();
					int counter = 0;

					while (true) {
						for (String key : keys) {
							System.out.println(key);
						}

						// An iteration also ends at "0"
						if (nextCursor.equals("0")) {
							break;
						}

						scanResult = jedis.scan(nextCursor, params);
						nextCursor = scanResult.getStringCursor();
						keys = scanResult.getResult();
					}
					//
					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (jedis != null) {
				try {
					returnConnection(jedis, error);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			log.info("task finished in " + (System.currentTimeMillis() - elapsed) + "ms");
		}

		System.out.println("----");
	}

	/**
	 * Start this component and implement the requirements of
	 * {@link org.apache.catalina.util.LifecycleBase#startInternal()}.
	 *
	 * @exception LifecycleException
	 *                if this component detects a fatal error that prevents this
	 *                component from being used
	 */
	@Override
	protected synchronized void startInternal() throws LifecycleException {
		super.startInternal();

		scheduler.scheduleAtFixedRate(this, 5, 5, TimeUnit.SECONDS);

		originalHostAndPort = defaultHostAndPort = new HostAndPort(host, port);
		connectionPools = new HashMap<>();

		setState(LifecycleState.STARTING);

		Boolean attachedToValve = false;
		for (Valve valve : getContext().getPipeline().getValves()) {
			if (valve instanceof RedisSessionHandlerValve) {
				this.handlerValve = (RedisSessionHandlerValve) valve;
				this.handlerValve.setRedisSessionManager(this);
				log.info("Attached to RedisSessionHandlerValve");
				attachedToValve = true;
				break;
			}
		}

		if (!attachedToValve) {
			String error = "Unable to attach to session handling valve; sessions cannot be saved after the request without the valve starting properly.";
			log.fatal(error);
			throw new LifecycleException(error);
		}

		try {
			initializeSerializer();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			log.fatal("Unable to load serializer", e);
			throw new LifecycleException(e);
		}

		log.info("Will expire sessions after " + (getContext().getSessionTimeout() * 60) + " seconds");

		initializeDatabaseConnection();

		getContext().setDistributable(true);

	}

	/**
	 * Stop this component and implement the requirements of
	 * {@link org.apache.catalina.util.LifecycleBase#stopInternal()}.
	 *
	 * @exception LifecycleException
	 *                if this component detects a fatal error that prevents this
	 *                component from being used
	 */
	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		if (log.isDebugEnabled()) {
			log.debug("Stopping");
		}

		scheduler.shutdown();

		setState(LifecycleState.STOPPING);

		for (Pool<Jedis> pool : connectionPools.values()) {
			try {
				pool.destroy();
			} catch (Exception e) {
				// Do nothing.
			}
		}

		// Require a new random number generator if we are restarted
		super.stopInternal();
	}

	protected String getOrGenerateSessionId(String requestedSessionId) {
		String jvmRoute = getJvmRoute();
		String sessionId = null;
		if (null != requestedSessionId) {
			sessionId = sessionIdWithJvmRoute(requestedSessionId, jvmRoute);
		} else {
			sessionId = sessionIdWithJvmRoute("redis-" + generateSessionId(), jvmRoute);
		}
		return sessionId;
	}

	@Override
	public Session createSession(String requestedSessionId) {
		// log.info("new session ------------- \n\n\n");
		RedisSession session = null;
		String sessionId = null;
		String jvmRoute = getJvmRoute();

		Boolean error = true;
		Jedis jedis = null;
		try {

			sessionId = getOrGenerateSessionId(requestedSessionId);

			long session_create_return_code = 0;
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount * 100) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					// Ensure generation of a unique session identifier.

					do {

						if (session_create_return_code == 0) {
							if (log.isDebugEnabled())
								log.debug("new session : " + sessionId);
							session_create_return_code = jedis.setnx(sessionId.getBytes(), NULL_SESSION);
						}
						if (session_create_return_code != 0) {
							expireAsync(sessionId, getContext().getSessionTimeout() * 60);
						} else {
							sessionId = getOrGenerateSessionId(requestedSessionId);
							if (log.isDebugEnabled())
								log.debug("setnx = 0. generate new : " + sessionId);
						}

					} while (session_create_return_code == 0L); // 1 = key set; 0 = key already

					if (log.isDebugEnabled())
						log.debug("completed in " + retryCount);

					break;

				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					if (log.isDebugEnabled())
						log.debug("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}

			/*
			 * Even though the key is set in Redis, we are not going to flag the current
			 * thread as having had the session persisted since the session isn't actually
			 * serialized to Redis yet. This ensures that the save(session) at the end of
			 * the request will serialize the session into Redis with 'set' instead of
			 * 'setnx'.
			 */

			error = false;

			if (null != sessionId) {
				session = (RedisSession) createEmptySession();
				session.setNew(true);
				session.setValid(true);
				session.setCreationTime(System.currentTimeMillis());
				session.setMaxInactiveInterval(getContext().getSessionTimeout() * 60);
				session.setId(sessionId);
				session.tellNew();
			}

			currentSession.set(session);
			currentSessionId.set(sessionId);
			currentSessionIsPersisted.set(false);
			currentSessionSerializationMetadata.set(new SessionSerializationMetadata());

			if (null != session) {
				try {
					error = saveInternal(jedis, session, true);
				} catch (IOException ex) {
					log.error("Error saving newly created session: " + ex.getMessage());
					currentSession.set(null);
					currentSessionId.set(null);
					session = null;
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}

		return session;
	}

	private String sessionIdWithJvmRoute(String sessionId, String jvmRoute) {
		if (jvmRoute != null) {
			String jvmRoutePrefix = '.' + jvmRoute;
			return sessionId.endsWith(jvmRoutePrefix) ? sessionId : sessionId + jvmRoutePrefix;
		}
		return sessionId;
	}

	@Override
	public Session createEmptySession() {
		return new RedisSession(this);
	}

	@Override
	public void add(Session session) {
		try {
			save(session);
		} catch (IOException ex) {
			log.warn("Unable to add to session manager store: " + ex.getMessage());
			throw new RuntimeException("Unable to add to session manager store.", ex);
		}
	}

	protected String getCurrentRequestURI() {
		Request request = RedisSessionHandlerValve.getCurrentRequest();
		if (request == null) {
			return "";
		}
		StringBuilder requestURI = new StringBuilder(request.getRequestURI());
		// if (request.getQueryString() != null) {
		// requestURI.append("?").append(request.getQueryString());
		// }
		return requestURI.toString();
	}

	@Override
	public Session findSession(String id) throws IOException {
		RedisSession session = null;

		if (null == id) {
			currentSessionIsPersisted.set(false);
			currentSession.set(null);
			currentSessionSerializationMetadata.set(null);
			currentSessionId.set(null);
		} else if (id.equals(currentSessionId.get())) {
			session = currentSession.get();
		} else {

			byte[] data = null;

			String requestURI = getCurrentRequestURI();
			if (ignorePattern != null && ignorePattern.matcher(requestURI.toString()).matches()) {
				if (log.isDebugEnabled())
					log.debug("requestURI[" + requestURI + "] matches ignore pattern. returning null session");
				data = null;
			} else {
				data = loadSessionDataFromRedis(id);
			}

			if (data != null) {
				DeserializedSessionContainer container = sessionFromSerializedData(id, data);
				session = container.session;
				currentSession.set(session);
				currentSessionSerializationMetadata.set(container.metadata);
				currentSessionIsPersisted.set(true);
				currentSessionId.set(id);
			} else {
				currentSessionIsPersisted.set(false);
				currentSession.set(null);
				currentSessionSerializationMetadata.set(null);
				currentSessionId.set(null);
			}
		}

		return session;
	}

	public void clear() {
		Jedis jedis = null;
		Boolean error = true;
		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				try {
					if (jedis != null) {
						returnConnection(jedis);
					}
					jedis = acquireConnection(defaultHostAndPort);
					jedis.flushDB();
					error = false;
					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public int getSize() throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				try {
					if (jedis != null) {
						returnConnection(jedis);
					}
					jedis = acquireConnection(defaultHostAndPort);
					int size = jedis.dbSize().intValue();
					error = false;
					return size;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public String[] keys() throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				try {
					if (jedis != null) {
						returnConnection(jedis);
					}
					jedis = acquireConnection(defaultHostAndPort);
					Set<String> keySet = jedis.keys("*");
					error = false;
					return keySet.toArray(new String[keySet.size()]);
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public byte[] loadSessionDataFromRedis(String id) throws IOException {

		long elapsed = System.currentTimeMillis();

		Jedis jedis = null;
		Boolean error = true;
		byte[] data = null;

		try {
			if (log.isTraceEnabled())
				log.trace("Attempting to load session " + id + " from Redis");

			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					data = jedis.get(id.getBytes());
					error = false;
					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}

			if (data == null) {
				if (log.isTraceEnabled())
					log.trace("Session " + id + " not found in Redis");
			}

			return data;
		} finally {
			log.info("load sessionid[" + id + "] data=[" + (data == null ? "null" : String.valueOf(data.length)) + "] "
					+ (System.currentTimeMillis() - elapsed) + "ms");

			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException {
		if (log.isTraceEnabled())
			log.trace("Deserializing session " + id + " from Redis");

		if (Arrays.equals(NULL_SESSION, data)) {
			log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
			throw new IOException("Serialized session data was equal to NULL_SESSION");
		}

		RedisSession session = null;
		SessionSerializationMetadata metadata = new SessionSerializationMetadata();

		try {
			session = (RedisSession) createEmptySession();

			serializer.deserializeInto(data, session, metadata);

			session.setId(id);
			session.setNew(false);
			session.setMaxInactiveInterval(getContext().getSessionTimeout() * 60);
			session.access();
			session.setValid(true);
			session.resetDirtyTracking();

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + id + "]:");
				Enumeration en = session.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}
		} catch (ClassNotFoundException ex) {
			log.fatal("Unable to deserialize into session", ex);
			throw new IOException("Unable to deserialize into session", ex);
		}

		return new DeserializedSessionContainer(session, metadata);
	}

	public void save(Session session) throws IOException {
		save(session, false);
	}

	public void save(Session session, boolean forceSave) throws IOException {
		Jedis jedis = null;
		Boolean error = true;

		long elapsed = System.currentTimeMillis();

		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					error = saveInternal(jedis, session, forceSave);
					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} catch (IOException e) {
			throw e;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
			if (log.isDebugEnabled())
				log.debug("save finished in " + (System.currentTimeMillis() - elapsed) + "ms");
		}
	}

	protected boolean saveInternal(Jedis jedis, Session session, boolean forceSave) throws IOException {
		boolean error = true;

		long elapsed = System.currentTimeMillis();

		try {
			if (log.isTraceEnabled())
				log.trace("Saving session " + session + " into Redis");

			RedisSession redisSession = (RedisSession) session;

			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + redisSession.getId() + "]:");
				Enumeration en = redisSession.getAttributeNames();
				while (en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}

			byte[] binaryId = redisSession.getId().getBytes();

			Boolean isCurrentSessionPersisted;
			SessionSerializationMetadata sessionSerializationMetadata = currentSessionSerializationMetadata.get();
			byte[] originalSessionAttributesHash = sessionSerializationMetadata.getSessionAttributesHash();
			byte[] sessionAttributesHash = null;
			if (forceSave || redisSession.isDirty()
					|| null == (isCurrentSessionPersisted = this.currentSessionIsPersisted.get())
					|| !isCurrentSessionPersisted || !Arrays.equals(originalSessionAttributesHash,
							(sessionAttributesHash = serializer.attributesHashFrom(redisSession)))) {

				if (log.isTraceEnabled())
					log.trace("Save was determined to be necessary");

				if (null == sessionAttributesHash) {
					sessionAttributesHash = serializer.attributesHashFrom(redisSession);
				}

				SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
				updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);

				jedis.set(binaryId, serializer.serializeFrom(redisSession, updatedSerializationMetadata));

				redisSession.resetDirtyTracking();
				currentSessionSerializationMetadata.set(updatedSerializationMetadata);
				currentSessionIsPersisted.set(true);
			} else {
				if (log.isTraceEnabled())
					log.trace("Save was determined to be unnecessary");
			}

			if (log.isTraceEnabled())
				log.trace("Setting expire timeout on session [" + redisSession.getId() + "] to "
						+ (getContext().getSessionTimeout() * 60));

			expireAsync(redisSession.getId(), (getContext().getSessionTimeout() * 60));

			// jedis.expire(binaryId, (getContext().getSessionTimeout() * 60));

			error = false;

			return error;
		} catch (Exception e) {
			log.error(e.getMessage());
			throw e;
		} finally {
			if (log.isDebugEnabled())
				log.debug("saved in " + (System.currentTimeMillis() - elapsed) + "ms");
		}
	}

	protected void expireAsync(final String sessionId, final int expire) {
		scheduler.submit(() -> {
			expire(sessionId, expire);
		});
	}

	protected void expire(String sessionId, int expire) {
		Jedis jedis = null;
		Boolean error = true;

		if (log.isTraceEnabled())
			log.trace("marking session ID : " + sessionId + " expire in " + expire + " seconds");

		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					jedis.expire(sessionId.getBytes(), expire);
					error = false;

					if (log.isDebugEnabled())
						log.debug(" done in " + retryCount);

					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	@Override
	public void remove(Session session) {
		remove(session, false);
	}

	@Override
	public void remove(Session session, boolean update) {
		Jedis jedis = null;
		Boolean error = true;

		if (log.isTraceEnabled())
			log.trace("Removing session ID : " + session.getId());

		try {
			for (int retryCount = 0;; retryCount++) {
				if (retryCount >= maxRetryCount) {
					throw new RuntimeException("too many retries. failed after " + maxRetryCount + " retries");
				}
				if (jedis != null) {
					returnConnection(jedis);
				}
				jedis = acquireConnection(defaultHostAndPort);
				try {
					jedis.del(session.getId());
					error = false;
					break;
				} catch (JedisRedirectionException e) {
					defaultHostAndPort = e.getTargetNode();
					log.info("redirected to " + defaultHostAndPort + ". updated as default");
					continue;
				} catch (Exception e) {
					retireCurrentPool(defaultHostAndPort);
					if (defaultHostAndPort.equals(originalHostAndPort) == false) {
						defaultHostAndPort = originalHostAndPort;
						log.warn("trying to recover error by trying " + originalHostAndPort, e);
						continue;
					} else {
						throw e;
					}
				}
			}
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public void afterRequest() {
		RedisSession redisSession = currentSession.get();
		try {
			if (redisSession != null) {
				if (redisSession.isValid()) {
					if (log.isTraceEnabled())
						log.trace("Request with session completed, saving session " + redisSession.getId());
					save(redisSession, getAlwaysSaveAfterRequest());
				} else {
					if (log.isTraceEnabled())
						log.trace("HTTP Session has been invalidated, removing :" + redisSession.getId());
					remove(redisSession);
				}
			}
		} catch (Exception e) {
			log.error("Error storing/removing session", e);
		} finally {
			currentSession.remove();
			currentSessionId.remove();
			currentSessionIsPersisted.remove();
			currentSessionSerializationMetadata.remove();
			if (log.isTraceEnabled())
				log.trace("Session removed from ThreadLocal :" + redisSession.getIdInternal());
		}
	}

	@Override
	public void processExpires() {
		// We are going to use Redis's ability to expire keys for session expiration.

		// Do nothing.
	}

	private Pool<Jedis> initializeDatabaseConnection() throws LifecycleException {
		return initializeDatabaseConnection(defaultHostAndPort);
	}

	private Pool<Jedis> initializeDatabaseConnection(String host, int port) throws LifecycleException {
		return initializeDatabaseConnection(new HostAndPort(host, port));
	}

	private Pool<Jedis> initializeDatabaseConnection(HostAndPort hostAndPort) throws LifecycleException {
		long elapsed = System.currentTimeMillis();
		try {
			if (getSentinelMaster() != null) {
				Set<String> sentinelSet = getSentinelSet();
				if (sentinelSet != null && sentinelSet.size() > 0) {
					connectionPool = new JedisSentinelPool(getSentinelMaster(), sentinelSet, this.connectionPoolConfig,
							getTimeout(), getPassword());
					connectionPools.put(hostAndPort, connectionPool);
				} else {
					throw new LifecycleException(
							"Error configuring Redis Sentinel connection pool: expected both `sentinelMaster` and `sentiels` to be configured");
				}
			} else {
				connectionPool = new JedisPool(this.connectionPoolConfig, hostAndPort.getHost(), hostAndPort.getPort(),
						getTimeout(), getPassword());
				connectionPools.put(hostAndPort, connectionPool);
			}
			return connectionPool;
		} catch (Exception e) {
			throw new LifecycleException("Error connecting to Redis", e);
		} finally {
			if (log.isDebugEnabled())
				log.debug("initializeDatabaseConnection(" + hostAndPort + ") finished in "
						+ (System.currentTimeMillis() - elapsed) + "ms");
		}
	}

	private void initializeSerializer() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		log.info("Attempting to use serializer :" + serializationStrategyClass);
		serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();

		Loader loader = null;

		if (getContext() != null) {
			loader = getContext().getLoader();
		}

		ClassLoader classLoader = null;

		if (loader != null) {
			classLoader = loader.getClassLoader();
		}
		serializer.setClassLoader(classLoader);
	}

	// Connection Pool Config Accessors

	// - from org.apache.commons.pool2.impl.GenericObjectPoolConfig

	public int getConnectionPoolMaxTotal() {
		return this.connectionPoolConfig.getMaxTotal();
	}

	public void setConnectionPoolMaxTotal(int connectionPoolMaxTotal) {
		this.connectionPoolConfig.setMaxTotal(connectionPoolMaxTotal);
	}

	public int getConnectionPoolMaxIdle() {
		return this.connectionPoolConfig.getMaxIdle();
	}

	public void setConnectionPoolMaxIdle(int connectionPoolMaxIdle) {
		this.connectionPoolConfig.setMaxIdle(connectionPoolMaxIdle);
	}

	public int getConnectionPoolMinIdle() {
		return this.connectionPoolConfig.getMinIdle();
	}

	public void setConnectionPoolMinIdle(int connectionPoolMinIdle) {
		this.connectionPoolConfig.setMinIdle(connectionPoolMinIdle);
	}

	// - from org.apache.commons.pool2.impl.BaseObjectPoolConfig

	public boolean getLifo() {
		return this.connectionPoolConfig.getLifo();
	}

	public void setLifo(boolean lifo) {
		this.connectionPoolConfig.setLifo(lifo);
	}

	public long getMaxWaitMillis() {
		return this.connectionPoolConfig.getMaxWaitMillis();
	}

	public void setMaxWaitMillis(long maxWaitMillis) {
		this.connectionPoolConfig.setMaxWaitMillis(maxWaitMillis);
	}

	public long getMinEvictableIdleTimeMillis() {
		return this.connectionPoolConfig.getMinEvictableIdleTimeMillis();
	}

	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
		this.connectionPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
	}

	public long getSoftMinEvictableIdleTimeMillis() {
		return this.connectionPoolConfig.getSoftMinEvictableIdleTimeMillis();
	}

	public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis) {
		this.connectionPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
	}

	public int getNumTestsPerEvictionRun() {
		return this.connectionPoolConfig.getNumTestsPerEvictionRun();
	}

	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
		this.connectionPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
	}

	public boolean getTestOnCreate() {
		return this.connectionPoolConfig.getTestOnCreate();
	}

	public void setTestOnCreate(boolean testOnCreate) {
		this.connectionPoolConfig.setTestOnCreate(testOnCreate);
	}

	public boolean getTestOnBorrow() {
		return this.connectionPoolConfig.getTestOnBorrow();
	}

	public void setTestOnBorrow(boolean testOnBorrow) {
		this.connectionPoolConfig.setTestOnBorrow(testOnBorrow);
	}

	public boolean getTestOnReturn() {
		return this.connectionPoolConfig.getTestOnReturn();
	}

	public void setTestOnReturn(boolean testOnReturn) {
		this.connectionPoolConfig.setTestOnReturn(testOnReturn);
	}

	public boolean getTestWhileIdle() {
		return this.connectionPoolConfig.getTestWhileIdle();
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.connectionPoolConfig.setTestWhileIdle(testWhileIdle);
	}

	public long getTimeBetweenEvictionRunsMillis() {
		return this.connectionPoolConfig.getTimeBetweenEvictionRunsMillis();
	}

	public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
		this.connectionPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
	}

	public String getEvictionPolicyClassName() {
		return this.connectionPoolConfig.getEvictionPolicyClassName();
	}

	public void setEvictionPolicyClassName(String evictionPolicyClassName) {
		this.connectionPoolConfig.setEvictionPolicyClassName(evictionPolicyClassName);
	}

	public boolean getBlockWhenExhausted() {
		return this.connectionPoolConfig.getBlockWhenExhausted();
	}

	public void setBlockWhenExhausted(boolean blockWhenExhausted) {
		this.connectionPoolConfig.setBlockWhenExhausted(blockWhenExhausted);
	}

	public boolean getJmxEnabled() {
		return this.connectionPoolConfig.getJmxEnabled();
	}

	public void setJmxEnabled(boolean jmxEnabled) {
		this.connectionPoolConfig.setJmxEnabled(jmxEnabled);
	}

	public String getJmxNameBase() {
		return this.connectionPoolConfig.getJmxNameBase();
	}

	public void setJmxNameBase(String jmxNameBase) {
		this.connectionPoolConfig.setJmxNameBase(jmxNameBase);
	}

	public String getJmxNamePrefix() {
		return this.connectionPoolConfig.getJmxNamePrefix();
	}

	public void setJmxNamePrefix(String jmxNamePrefix) {
		this.connectionPoolConfig.setJmxNamePrefix(jmxNamePrefix);
	}

	public int getMaxRetryCount() {
		return maxRetryCount;
	}

	public void setMaxRetryCount(int maxRetryCount) {
		this.maxRetryCount = maxRetryCount;
	}

	public Pattern getIgnorePattern() {
		return ignorePattern;
	}

	public void setIgnorePattern(String ignorePattern) {
		if (ignorePattern != null && ignorePattern.isEmpty() == false) {
			this.ignorePattern = Pattern.compile(ignorePattern);
		}
	}
}

class DeserializedSessionContainer {
	public final RedisSession session;
	public final SessionSerializationMetadata metadata;

	public DeserializedSessionContainer(RedisSession session, SessionSerializationMetadata metadata) {
		this.session = session;
		this.metadata = metadata;
	}
}

package moar;

import static java.lang.System.currentTimeMillis;
import static java.lang.reflect.Proxy.newProxyInstance;
import static java.sql.DriverManager.registerDriver;
import static moar.Cost.$;
import static moar.JsonUtil.trace;
import static moar.JsonUtil.warn;
import static org.slf4j.LoggerFactory.getLogger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Driver with the ability to run scripts and recover from connection errors
 */
public class Driver
    implements
    java.sql.Driver {
  private static final RateLimiter retryRateLimiter = RateLimiter.create(1);
  private static final PropertyAccessor props = new PropertyAccessor(Driver.class.getName());
  private static final int CONNECTION_RETRY_LIMIT = props.getInteger("connectionRetryLimit", 100);
  private static final ClassLoader classLoader = Driver.class.getClassLoader();
  private static Map<String, Future<Callable<Connection>>> connectionSource = new HashMap<>();
  private final static Logger LOG = getLogger(Driver.class);
  private static final ExecutorService executorService = MoreExecutors.newDirectExecutorService();
  static {
    try {
      registerDriver(new Driver());
    } catch (final SQLException e) {
      throw new RuntimeException("failure static init " + Driver.class, e);
    }
  }
  private static final java.util.logging.Logger javaLogger = java.util.logging.Logger.getLogger(Driver.class.getName());
  private static final String PREFIX = "moar:";
  private static final long VALID_CHECK_MILLIS = 1000 * 60;
  private final DriverPropertyInfo[] driverProps = new DriverPropertyInfo[] {};
  private final HashMap<String, ConnectionSpec> failFast = new HashMap<>();
  private Timer recovery;
  private final long restMillis = 1000;

  private final TimerTask recoveryTask = new TimerTask() {
    private final long failFastRecoveryLimit = 1000 * 60 * 5;

    @Override
    public void run() {
      final Iterator<ConnectionSpec> i = failFast.values().iterator();
      while (i.hasNext()) {
        final ConnectionSpec item = i.next();
        if (currentTimeMillis() - item.createdMillis() > failFastRecoveryLimit) {
          failFast.remove(item.toString());
        } else {
          try {
            doConnect(item).close();
          } catch (final SQLException e) {
            warn(LOG, e.getMessage(), e);
          }
        }
      }
      if (failFast.size() == 0) {
        final Timer removed = recovery;
        recovery = null;
        removed.cancel();
      }
    }
  };

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    final boolean startsWith = url.startsWith(PREFIX);
    return startsWith;
  }

  private Connection checkConnection(final AtomicReference<Connection> connection, final ConnectionSpec cs)
      throws SQLException {
    if (currentTimeMillis() - cs.getValidCheck().get() > VALID_CHECK_MILLIS) {
      ensureConnectionIsValid(cs, connection);
    }
    return connection.get();
  }

  @Override
  public Connection connect(final String url, final Properties props) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    final int pLen = PREFIX.length();
    final int p = url.indexOf(":", pLen);
    final String config = url.substring(pLen, p);
    final String backendUrl = url.substring(p + 1);
    final ConnectionSpec cs = new ConnectionSpec(backendUrl, props, config);
    final ConnectionSpec fcs = failFast.get(cs.toString());
    if (null != fcs) {
      final String msg = "The connection specification is in fail fast mode and has not yet recovered.";
      throw new SQLException(msg);
    }
    try {
      return doConnect(cs);
    } catch (final SQLException e) {
      failFast.put(cs.toString(), cs);
      startRecovery();
      throw e;
    }
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final PreparedStatement s = c.prepareStatement(sql);
    return $("data", PreparedStatement.class, s);
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql, final int i1) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final PreparedStatement s = c.prepareStatement(sql, i1);
    return $("data", PreparedStatement.class, s);
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql, final int resultSetType, final int resultSetConcurrency)
      throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final PreparedStatement s = c.prepareStatement(sql, resultSetType, resultSetConcurrency);
    return $("data", PreparedStatement.class, s);
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql, final int resultSetType, final int resultSetConcurrency,
      final int resultSetHoldability) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final PreparedStatement s = c.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    return $("data", PreparedStatement.class, s);
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql, final Integer[] i1) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final int[] primitiveArray = new int[i1.length];
    for (int i = 0; i < i1.length; i++) {
      primitiveArray[i] = i1[i];
    }
    final PreparedStatement s = c.prepareStatement(sql, primitiveArray);
    return $("data", PreparedStatement.class, s);
  }

  private PreparedStatement createPreparedStatement(final AtomicReference<Connection> connection,
      final ConnectionSpec cs, final String sql, final String[] p1) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final PreparedStatement s = c.prepareStatement(sql, p1);
    return $("data", PreparedStatement.class, s);
  }

  private Statement createStatement(final AtomicReference<Connection> connection, final ConnectionSpec cs)
      throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final Statement s = c.createStatement();
    return $("data", Statement.class, s);
  }

  private Statement createStatement(final AtomicReference<Connection> connection, final ConnectionSpec cs,
      final int resultSetType, final int resultSetConcurrency) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final Statement s = c.createStatement(resultSetType, resultSetConcurrency);
    return $("data", Statement.class, s);
  }

  private Statement createStatement(final AtomicReference<Connection> connection, final ConnectionSpec cs,
      final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
    final Connection c = checkConnection(connection, cs);
    final Statement s = c.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    return $("data", Statement.class, s);
  }

  private Connection doConnect(final ConnectionSpec cs) throws SQLException {
    init(cs);
    failFast.remove(cs.getUrl());
    return proxy(cs);
  }

  private void ensureConnectionIsValid(final ConnectionSpec cs, final AtomicReference<Connection> connection)
      throws SQLException {
    if (isAutoCommit(connection)) {
      // Don't mess with a transactional connection!
      return;
    }
    int retries = CONNECTION_RETRY_LIMIT;
    while (!isValid(cs, connection) && retries-- > 0) {
      retryRateLimiter.acquire();
      try {
        connection.get().close();
      } catch (final SQLException e) {
        warn(LOG, "unable to close");
      }
      connection.set(getConnectionFromSource(cs));
    }
    cs.getValidCheck().set(currentTimeMillis());
  }

  private Connection getConnectionFromSource(final ConnectionSpec cs) {
    Connection c = null;
    while (c == null) {
      try {
        synchronized (connectionSource) {
          c = connectionSource.get(cs.getUrl()).get().call();
        }
      } catch (final InterruptedException e) {
        warn(LOG, e.getMessage());
      } catch (final ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }
        throw new RuntimeException(e);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
    return $("data", Connection.class, c);
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return javaLogger;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(final String arg0, final Properties arg1) throws SQLException {
    return driverProps;
  }

  private Connection getRealConnection(final ConnectionSpec cs) throws SQLException {
    final String url = cs.getUrl();
    final Properties props = cs.getProps();
    int retries = CONNECTION_RETRY_LIMIT;
    SQLException lastException = null;
    while (retries-- > 0) {
      try {
        return DriverManager.getConnection(url, props);
      } catch (final SQLException e) {
        warn(LOG, retries, e.getMessage());
        lastException = e;
        retryRateLimiter.acquire(1);
      }
    }
    throw lastException;
  }

  private void init(final ConnectionSpec cs) throws SQLException {
    final String url = cs.getUrl();
    final String config = cs.getConfig();
    synchronized (connectionSource) {
      if (connectionSource.get(url) == null) {
        final Future<Callable<Connection>> f = $(new Cost.AsyncProvider() {
          @Override
          public <T> Future<T> submit(final Callable<T> c) {
            return executorService.submit(c);
          }
        }, () -> {
          try (Connection c = getRealConnection(cs)) {
            final ScriptManager s = new ScriptManager(config, url, c);
            s.init();
          }
          final Callable<Connection> callable = () -> getRealConnection(cs);
          return callable;
        });
        connectionSource.put(url, f);
      }
    }
  }

  private boolean isAutoCommit(final AtomicReference<Connection> connection) throws SQLException {
    return connection.get().getAutoCommit() == false;
  }

  private boolean isValid(final ConnectionSpec cs, final AtomicReference<Connection> connection) {
    final Connection cn = connection.get();
    try (Statement s = cn.createStatement()) {
      return null != s.executeQuery("select 1");
    } catch (final SQLException e) {
      String desc = e.getMessage();
      final String timeoutMsg = "The last packet successfully received from the server was";
      if (desc.startsWith(timeoutMsg)) {
        desc = desc.substring(timeoutMsg.length(), desc.indexOf("milliseconds ago.")) + "ms";
      }
      warn(LOG, e.getErrorCode(), e.getSQLState(), desc, cs.getUrl());
      return false;
    }
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  private Connection proxy(final ConnectionSpec cs) throws SQLException {
    final AtomicReference<Connection> connection = new AtomicReference<>();
    connection.set(getConnectionFromSource(cs));
    return (Connection) newProxyInstance(classLoader, new Class<?>[] { Connection.class }, (proxy, method, a) -> {
      try {
        final String methodName = method.getName();
        if (methodName.equals("close")) {
          if (connection.get() != null) {
            trace(LOG, "close", cs.getUrl());
            connection.get().close();
            connection.set(null);
          }
          return null;
        } else if (methodName.equals("isValid")) {
          return true;
        } else if (methodName.equals("createStatement")) {
          if (a == null || a.length == 0) {
            return createStatement(connection, cs);
          } else if (a.length == 2) {
            return createStatement(connection, cs, (Integer) a[0], (Integer) a[1]);
          } else {
            return createStatement(connection, cs, (Integer) a[0], (Integer) a[1], (Integer) a[2]);
          }
        } else if (methodName.equals("prepareStatement")) {
          if (a.length == 1) {
            return createPreparedStatement(connection, cs, (String) a[0]);
          } else if (a.length == 2 && a[1] instanceof Integer) {
            return createPreparedStatement(connection, cs, (String) a[0], (Integer) a[1]);
          } else if (a.length == 2 && a[1] instanceof Integer[]) {
            return createPreparedStatement(connection, cs, (String) a[0], (Integer[]) a[1]);
          } else if (a.length == 2 && a[1] instanceof String[]) {
            return createPreparedStatement(connection, cs, (String) a[0], (String[]) a[1]);
          } else if (a.length == 3) {
            return createPreparedStatement(connection, cs, (String) a[0], (int) a[1], (int) a[2]);
          } else {
            return createPreparedStatement(connection, cs, (String) a[0], (int) a[1], (int) a[2], (int) a[3]);
          }
        } else if (methodName.equals("prepareStatement") && a.length == 1) {
          return createPreparedStatement(connection, cs, (String) a[0]);
        } else if (cs.getMetaData() != null && methodName.equals("getMetaData")) {
          return cs.getMetaData();
        } else {
          final Connection c = checkConnection(connection, cs);
          final Object result = method.invoke(c, a);
          if (methodName.equals("getMetaData")) {
            cs.setMetaData((DatabaseMetaData) result);
          }
          return result;
        }
      } catch (InvocationTargetException | UndeclaredThrowableException e) {
        throw e.getCause();
      }
    });

  }

  private synchronized void startRecovery() {
    if (recovery != null) {
      return;
    }
    recovery = new Timer(getClass().getName() + ":Recovery");
    recovery.schedule(recoveryTask, 0, restMillis);
  }

}

package moar;
import static java.lang.System.currentTimeMillis;
import static moar.Cost.$;
import static moar.JsonUtil.info;
import static moar.JsonUtil.warn;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScriptManager {

  private final static Logger LOG = LoggerFactory.getLogger(ScriptManager.class);
  private static Class<?> loader = ScriptManager.class;
  private static final PropertyAccessor props = new PropertyAccessor(ScriptManager.class.getName());
  private static final long timeoutMillis = props.getLong("timeoutMillis", 1000 * 60 * 5L);
  private static final Object scriptRoot = props.getString("scriptRoot", "/sql");

  public static void setLoader(final Class<?> loader) {
    ScriptManager.loader = loader;
  }

  private final int tableDoesNotExistErrorCode;
  private final String track;
  private final Connection connection;
  private final long restPeriod = 1000 * 1;
  private final long startMillis;

  private final String instance = UUID.randomUUID().toString();

  ScriptManager(final String config, final String url, final Connection connection) throws SQLException {
    info(LOG, "moar-driver-script-manager", instance);
    this.connection = connection;
    if (connection instanceof com.mysql.cj.jdbc.ConnectionImpl) {
      tableDoesNotExistErrorCode = 1146;
    } else {
      final String msg = connection.getClass().getName() + " not currently supported";
      throw new SQLException(msg);
    }
    startMillis = currentTimeMillis();
    final String[] param = config.split(";");
    int i = 0;
    track = i < param.length ? param[i++] : "default";
  }

  private void execute(final PreparedStatement find, final PreparedStatement register, final Statement statement,
      final PreparedStatement record) throws SQLException, IOException {
    try {
      $("running db scripts", () -> {
        int id;
        while (-1 != (id = find(find, register, statement, record))) {
          final int scriptId = id;
          info(LOG, "attempting to run script ", id);
          if (!$("script " + id, () -> run(register, statement, record, scriptId))) {
            warn(LOG, "unable to run (will retry)", id);
            rest(scriptId);
          }
        }
        return null;
      });
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int find(final PreparedStatement find, final PreparedStatement register, final Statement statement,
      final PreparedStatement record) throws SQLException, IOException {
    int id;
    try {
      try (final ResultSet r = find.executeQuery()) {
        id = r.next() ? r.getInt(1) + 1 : 1000;
      }
    } catch (final SQLException ex) {
      if (tableDoesNotExistErrorCode != ex.getErrorCode()) {
        throw ex;
      }
      String sql = "CREATE TABLE `%s` (`id` BIGINT, `instance` VARCHAR(255),`run_event` VARCHAR(255),";
      sql += "`created` DATETIME,`completed` DATETIME, PRIMARY KEY(`id`));";
      sql = String.format(sql, "moar_" + track);
      statement.execute(sql);
      id = 1000;
    }
    if (getResource(id) != null) {
      return id;
    }
    return -1;
  }

  private InputStream getResource(final int id) {
    final String resource = String.format("%s/%s/%d.sql", scriptRoot, track, id);
    return loader.getResourceAsStream(resource);
  }

  void init() throws SQLException {
    synchronized (Object.class) {
      String sql = "select `id` from `%s` where not isnull(`completed`) order by `id` desc";
      sql = String.format(sql, "moar_" + track);
      try (PreparedStatement find = connection.prepareStatement(sql)) {
        sql = "insert into `%s` (`id`, `instance`, `created`, `run_event`) values (?, ?, NOW(), ?)";
        sql = String.format(sql, "moar_" + track);
        try (PreparedStatement register = connection.prepareStatement(sql)) {
          sql = String.format("update `%s` set completed=NOW() WHERE id=?", "moar_" + track);
          try (PreparedStatement record = connection.prepareStatement(sql)) {
            try (Statement statement = connection.createStatement()) {
              execute(find, register, statement, record);
            }
          }
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void record(final PreparedStatement record, final int id) throws SQLException {
    record.setInt(1, id);
    record.execute();
  }

  private void register(final PreparedStatement register, final int id, final String runEvent) throws SQLException {
    int i = 0;
    register.setInt(++i, id);
    register.setString(++i, instance);
    register.setString(++i, runEvent);
    register.execute();
  }

  /**
   * The script manager goes into rest mode if it finds that it can not run a required script.
   * <p>
   * This can occur if another script manager is currently running scripts using the same track table. If this is the
   * case then resting will allow the other script manager to complete it's work.
   * <p>
   * If the other script manager never completes it's work then we can never allow our connection to proceed. One case
   * where this can be very bad is if a script manager crashes after registering a script and before recording the
   * script. If this is the case then we are stuck and after our time out we throw an SQL exception.
   * <p>
   * This occurs under conditions where multiple drivers attempt to start migrations at the same time.
   *
   * @param id
   * @throws SQLException
   */
  private void rest(final int id) throws SQLException {
    try {
      Thread.sleep(restPeriod);
    } catch (final InterruptedException e) {
      warn(LOG, e.getMessage());
    }
    if (currentTimeMillis() - startMillis > timeoutMillis) {
      final String msg = "Timeout while waiting on script " + id;
      throw new SQLException(msg);
    }
  }

  /**
   * Register the script
   * <p>
   * In theory it is possible that more then one script runner will attempt to do this at the exact time time with the
   * exact same script number (i.e. in a cluster environment we may have more then one system running at the same time).
   * <p>
   * Regardless of environment only one process can succeed in the race to register a script due to the database primary
   * key restriction.
   *
   * @param register
   * @param file
   * @throws Exception
   */
  private boolean run(final PreparedStatement register, final Statement statement, final PreparedStatement record,
      final int id) throws Exception {
    final String runEvent = UUID.randomUUID().toString();
    try {
      register(register, id, runEvent);
    } catch (final SQLException ex) {
      warn(LOG, "unable to register script (another instance may be running it)", id, ex.getErrorCode(),
          ex.getSQLState(), ex.getMessage());
      return false;
    }
    final StatementReader stream = new StatementReader(getResource(id));
    $("Running " + id, () -> {
      String sql;
      long statementNumber = 0;
      while (null != (sql = stream.readStatement())) {
        try {
          statement.execute(sql);
          statementNumber++;
        } catch (final SQLException e) {
          throw new JsonMessageException("script failed", id, statementNumber, instance, runEvent);
        }
      }
      return null;
    });
    record(record, id);
    return true;
  }
}

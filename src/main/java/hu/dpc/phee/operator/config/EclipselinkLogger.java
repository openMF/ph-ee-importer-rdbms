package hu.dpc.phee.operator.config;

import java.util.HashMap;
import java.util.Map;
import org.eclipse.persistence.logging.AbstractSessionLog;
import org.eclipse.persistence.logging.SessionLog;
import org.eclipse.persistence.logging.SessionLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * This is a wrapper class for SLF4J. It is used when messages need to be logged through SLF4J.
 */
public class EclipselinkLogger extends AbstractSessionLog {

    public static final String ECLIPSELINK_NAMESPACE = "org.eclipse.persistence.logging";
    public static final String DEFAULT_CATEGORY = "default";

    public static final String DEFAULT_ECLIPSELINK_NAMESPACE = ECLIPSELINK_NAMESPACE + "." + DEFAULT_CATEGORY;

    private Map<Integer, LogLevel> mapLevels;
    private Map<String, Logger> categoryLoggers = new HashMap<>();

    public EclipselinkLogger() {
        createCategoryLoggers();
        initMapLevels();
    }

    @Override
    public void log(SessionLogEntry entry) {
        if (!shouldLog(entry.getLevel(), entry.getNameSpace())) {
            return;
        }

        Logger logger = getLogger(entry.getNameSpace());
        LogLevel logLevel = getLogLevel(entry.getLevel());

        StringBuilder message = new StringBuilder();
        message.append(getSupplementDetailString(entry));
        message.append(formatMessage(entry));

        String finalMesssage = message.toString();
        switch (logLevel) {
            case TRACE:
                logger.trace(finalMesssage);
            break;
            case DEBUG:
                logger.debug(finalMesssage);
            break;
            case INFO:
                logger.info(finalMesssage);
            break;
            case WARN:
                logger.warn(finalMesssage);
            break;
            case ERROR:
                logger.error(finalMesssage);
            break;
            default:
            break;
        }
    }

    @Override
    public boolean shouldLog(int level, String category) {
        Logger logger = getLogger(category);
        boolean resp = false;
        LogLevel logLevel = getLogLevel(level);

        switch (logLevel) {
            case TRACE:
                resp = logger.isTraceEnabled();
            break;
            case DEBUG:
                resp = logger.isDebugEnabled();
            break;
            case INFO:
                resp = logger.isInfoEnabled();
            break;
            case WARN:
                resp = logger.isWarnEnabled();
            break;
            case ERROR:
                resp = logger.isErrorEnabled();
            break;
            default:
            break;
        }
        return resp;
    }

    @Override
    public boolean shouldLog(int level) {
        return shouldLog(level, DEFAULT_CATEGORY);
    }

    /**
     * Return true if SQL logging should log visible bind parameters. If the shouldDisplayData is not set, return false.
     */
    @Override
    public boolean shouldDisplayData() {
        if (this.shouldDisplayData != null) {
            return shouldDisplayData;
        } else {
            return false;
        }
    }

    /**
     * Initialize loggers eagerly.
     */
    private void createCategoryLoggers() {
        for (String category : SessionLog.loggerCatagories) {
            addLogger(category, ECLIPSELINK_NAMESPACE + "." + category);
        }
        addLogger(DEFAULT_CATEGORY, DEFAULT_ECLIPSELINK_NAMESPACE);
    }

    /**
     * INTERNAL: Add Logger to the categoryLoggers.
     */
    private void addLogger(String loggerCategory, String loggerNameSpace) {
        categoryLoggers.put(loggerCategory, LoggerFactory.getLogger(loggerNameSpace));
    }

    /**
     * INTERNAL: Return the Logger for the given category.
     */
    private Logger getLogger(String wantedCategory) {
        String category = wantedCategory;
        if (!StringUtils.hasText(wantedCategory) || !this.categoryLoggers.containsKey(wantedCategory)) {
            category = DEFAULT_CATEGORY;
        }
        return categoryLoggers.get(category);
    }

    /**
     * Return the corresponding Slf4j Level for a given EclipseLink level.
     */
    private LogLevel getLogLevel(Integer level) {
        LogLevel logLevel = mapLevels.get(level);
        if (logLevel == null) {
            logLevel = LogLevel.OFF;
        }
        return logLevel;
    }

    enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR, OFF
    }

    private void initMapLevels() {
        mapLevels = new HashMap<>();
        mapLevels.put(SessionLog.ALL, LogLevel.TRACE);
        mapLevels.put(SessionLog.FINEST, LogLevel.TRACE);
        mapLevels.put(SessionLog.FINER, LogLevel.TRACE);
        mapLevels.put(SessionLog.FINE, LogLevel.DEBUG);
        mapLevels.put(SessionLog.CONFIG, LogLevel.INFO);
        mapLevels.put(SessionLog.INFO, LogLevel.INFO);
        mapLevels.put(SessionLog.WARNING, LogLevel.WARN);
        mapLevels.put(SessionLog.SEVERE, LogLevel.ERROR);
    }
}

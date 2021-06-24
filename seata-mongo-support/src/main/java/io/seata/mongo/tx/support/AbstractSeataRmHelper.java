package io.seata.mongo.tx.support;


import static io.seata.common.DefaultValues.DEFAULT_CLIENT_REPORT_RETRY_COUNT;
import static io.seata.common.DefaultValues.DEFAULT_CLIENT_REPORT_SUCCESS_ENABLE;

import com.google.common.base.Joiner;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO.OpeType;
import io.seata.mongo.tx.mongo.util.MongoQueryUtil;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.exec.BaseTransactionalExecutor;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import io.seata.rm.datasource.util.JdbcUtils;
import io.seata.sqlparser.util.JdbcConstants;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

/**
 * @description: 管理seata分支和context
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/17 17:00
 * @version:v1.0
 */
@Slf4j
public abstract class AbstractSeataRmHelper implements UndoLogStorable {

    public static final boolean IS_REPORT_SUCCESS_ENABLE = ConfigurationFactory.getInstance().getBoolean(
        ConfigurationKeys.CLIENT_REPORT_SUCCESS_ENABLE, DEFAULT_CLIENT_REPORT_SUCCESS_ENABLE);
    private static final int REPORT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_REPORT_RETRY_COUNT, DEFAULT_CLIENT_REPORT_RETRY_COUNT);

    @Resource
    private MongoLockRetryPolicy mongoLockRetryPolicy;
    private final String resourceId;

    public AbstractSeataRmHelper(DataSource dataSource) {
        /**
         * @see DataSourceProxy#getResourceId()
         */
        try (Connection conn = dataSource.getConnection()) {
            // 初始化的时候存储一份resourceId，暂时用mysql的resourceId
            String jdbcUrl = conn.getMetaData().getURL();
            if (jdbcUrl.contains("?")) {
                resourceId = jdbcUrl.substring(0, jdbcUrl.indexOf('?'));
            } else {
                resourceId = jdbcUrl;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("can not init dataSource", e);
        }
    }

    /**
     * 不同的db持有不同的context
     *
     * @return
     */
    public abstract ThreadLocal<ConnectionContext> getContext();

    /**
     * 初始化context，context标识该事务是否在一次全局事务下
     */
    public void doStartAndBindContext() {
        getContext().remove();
        String xid = RootContext.getXID();
        if (xid != null) {
            getContext().get().bind(xid);
        }
    }

    /**
     * 提交时注册分支（同时抢占锁）
     */
    public void doCommit() {
        try {
            if (getContext().get().inGlobalTransaction()) {
                mongoLockRetryPolicy.execute(() -> {
                    processGlobalTransactionCommit();
                    return null;
                });
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * commit之后通知TC一阶段提交成功
     */
    public void doSuccess() {
        // 一阶段提交成功，通知TC
        if (getContext().get().inGlobalTransaction() && IS_REPORT_SUCCESS_ENABLE) {
            try {
                report(true);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 事务执行失败，通知TC一阶段失败
     */
    public void doFailure() {
        if (getContext().get().inGlobalTransaction()) {
            try {
                report(false);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * lockKey的格式 "table:id1,id2"
     *
     * @param customUndoRecordBOS
     * @param collectionName
     * @see BaseTransactionalExecutor#buildLockKey(io.seata.rm.datasource.sql.struct.TableRecords)
     */
    protected void buildLockKey(Collection<CustomUndoRecordBO> customUndoRecordBOS, String collectionName) {
        if (CollectionUtils.isEmpty(customUndoRecordBOS)) {
            return;
        }

        if (getContext().get().inGlobalTransaction()) {
            Set<String> mongoIds = customUndoRecordBOS.stream()
                .map(bo -> OpeType.DELETE.equals(bo.getOpeType())
                    ? bo.getBeforeImage()
                    : bo.getAfterImage())
                .map(MongoQueryUtil::getMongoDataId)
                .collect(Collectors.toSet());
            String lockKey = collectionName + ":" + Joiner.on(",").join(mongoIds);
            getContext().get().appendLockKey(lockKey);
        }
    }

    /**
     * @return
     * @throws io.seata.core.exception.TransactionException
     * @see io.seata.rm.datasource.ConnectionProxy#register()
     */
    protected Long register() throws io.seata.core.exception.TransactionException, SQLException {
        String xid = RootContext.getXID();
        if (xid == null || CollectionUtils.isEmpty(getTempUndoLog(xid))) {
            return null;
        }

        // 手动注册branch到当前xid标识的全局事务会话中
        Long branchId = DefaultResourceManager.get().branchRegister(BranchType.AT, resourceId,
            null, xid, JdbcConstants.MONGO, getContext().get().buildLockKeys());
        getContext().get().setBranchId(branchId);

        String dbType = JdbcUtils.getDbType(resourceId);
        // 写入undolog到redis
        UndoLogManagerFactory.getUndoLogManager(dbType).flushUndoLogs(null);
        return branchId;

    }

    protected void processGlobalTransactionCommit() throws SQLException {
        try {
            register();
        } catch (io.seata.core.exception.TransactionException e) {
            recognizeLockKeyConflictException(e, getContext().get().buildLockKeys());
        } catch (SQLException sqlException) {
            throw sqlException;
        }
    }

    protected void recognizeLockKeyConflictException(io.seata.core.exception.TransactionException te, String lockKeys) throws SQLException {
        if (te.getCode() == TransactionExceptionCode.LockKeyConflict) {
            StringBuilder reasonBuilder = new StringBuilder("get global lock fail, xid:");
            reasonBuilder.append(getContext().get().getXid());
            if (StringUtils.isNotBlank(lockKeys)) {
                reasonBuilder.append(", lockKeys:").append(lockKeys);
            }

            // 暂不重试
            // 锁的格式 -- SEATA_ROW_LOCK_jdbc:mysql://192.168.1.243:3306/metadata_test8^^^24852d91-5c43-4f67-99c5-e6e441337749__object_3spBf__c^^^60a485dcdbb1c740d458ee9c
            // throw new LockConflictException("锁被占用，请重试 >> " + reasonBuilder);
            log.error(reasonBuilder.toString());
            // TODO 2021/6/10 使感知到提示
            throw new RuntimeException("retry ");
        } else {
            throw new RuntimeException(te);
        }
    }

    protected void report(boolean commitDone) throws SQLException {
        if (getContext().get().getBranchId() == null) {
            return;
        }
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            String xid = getContext().get().getXid();
            Long branchId = getContext().get().getBranchId();
            try {
                DefaultResourceManager.get().branchReport(BranchType.AT, xid, branchId,
                    commitDone ? BranchStatus.PhaseOne_Done : BranchStatus.PhaseOne_Failed, null);
                return;
            } catch (Throwable ex) {
                log.error("Failed to report [" + branchId + "/" + xid + "] commit done ["
                    + commitDone + "] Retry Countdown: " + retry);
                retry--;

                if (retry == 0) {
                    throw new SQLException("Failed to report branch status " + commitDone, ex);
                }
            }
        }
    }

}

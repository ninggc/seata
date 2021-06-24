package io.seata.mongo.tx.mongo.support;

import io.seata.common.loader.LoadLevel;
import io.seata.core.context.RootContext;
import io.seata.mongo.tx.mongo.component.MongoUndoLogService;
import io.seata.mongo.tx.mongo.component.SpringSeataTxSupportHelper;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.undo.mysql.MySQLUndoLogManager;
import io.seata.sqlparser.util.JdbcConstants;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: CustomUndoLogManager
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/12 16:42
 * @version:v1.0
 */
@LoadLevel(name = JdbcConstants.MYSQL)
@Slf4j
public class CustomUndoLogManager extends MySQLUndoLogManager {

    @Override
    public void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        String xid = RootContext.getXID();
        if (cp == null && xid != null) {
            // 自己管理的undolog没有传cp
            SpringSeataTxSupportHelper.getMongoUndoLogService().flushUndoLogs(xid);
        }

        if (cp != null) {
            super.flushUndoLogs(cp);
        }
    }

    @Override
    public int deleteUndoLogByLogCreated(Date logCreated, int limitRows, Connection conn) throws SQLException {
        getMongoUndoLogService().deleteUndoLogByLogCreated(logCreated);
        return super.deleteUndoLogByLogCreated(logCreated, limitRows, conn);
    }

    @Override
    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException {
        getMongoUndoLogService().batchDeleteUndoLog(xids, branchIds);
        super.batchDeleteUndoLog(xids, branchIds, conn);
    }

    private MongoUndoLogService getMongoUndoLogService() {
        return SpringSeataTxSupportHelper.getMongoUndoLogService();
    }
}

package io.seata.mongo.tx.mongo.support;

import static io.seata.core.model.BranchStatus.PhaseTwo_RollbackFailed_Unretryable;
import static io.seata.core.model.BranchStatus.PhaseTwo_Rollbacked;

import io.seata.core.exception.BranchTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.BranchStatus;
import io.seata.core.protocol.transaction.BranchRollbackRequest;
import io.seata.core.protocol.transaction.BranchRollbackResponse;
import io.seata.mongo.tx.mongo.component.SpringSeataTxSupportHelper;
import io.seata.mongo.tx.mongo.util.DirtyWriteException;
import io.seata.rm.RMHandlerAT;
import io.seata.sqlparser.util.JdbcConstants;
import lombok.extern.slf4j.Slf4j;

/**
 * @description: 对seata默认的回调处理的扩展，AT模式
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/7 16:27
 * @version:v1.0
 */
@Slf4j
public class MongoRmHandlerAT extends RMHandlerAT {

    /**
     * 因为mongo的分支是单独注册的，所以不会出现一个branch同时有mongo和mysql
     *
     * @param request
     * @param response
     * @throws TransactionException
     */
    @Override
    protected void doBranchRollback(BranchRollbackRequest request, BranchRollbackResponse response) throws TransactionException {
        String applicationData = request.getApplicationData();

        if (JdbcConstants.MONGO.equals(applicationData)) {
            // DO MONGO
            doMongoBranchRollback(request, response);
        } else {
            // DO MYSQL
            doMysqlBranchRollback(request, response);
        }
    }

    private void doMongoBranchRollback(BranchRollbackRequest request, BranchRollbackResponse response) throws BranchTransactionException {
        String xid = request.getXid();
        long branchId = request.getBranchId();
        String resourceId = request.getResourceId();
        if (log.isInfoEnabled()) {
            log.info("Branch Rollbacking: " + xid + " " + branchId + " " + resourceId);
        }

        BranchStatus branchStatus = PhaseTwo_Rollbacked;
        try {
            SpringSeataTxSupportHelper.getMongoUndoLogService().undo(xid, branchId);
        } catch (DirtyWriteException e) {
            log.error("rollback error", e);
            branchStatus = PhaseTwo_RollbackFailed_Unretryable;
        }

        response.setXid(xid);
        response.setBranchId(branchId);
        response.setBranchStatus(branchStatus);
        if (log.isInfoEnabled()) {
            log.info("Branch Rollbacked result: " + PhaseTwo_Rollbacked);
        }
    }

    private void doMysqlBranchRollback(BranchRollbackRequest request, BranchRollbackResponse response) throws TransactionException {
        super.doBranchRollback(request, response);
    }
}

package io.seata.mongo.tx.mongo.config;

import io.seata.mongo.tx.support.MongoSeataRmHelper;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * @description: 生成undolog的事务管理器
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/12 16:01
 * @version:v1.0
 */
@Component(ProxyMongoTransactionManager.BEAN_NAME)
@Slf4j
public class ProxyMongoTransactionManager extends MongoTransactionManager {
    public static final String BEAN_NAME = "mongoTransactionManager";

    @Resource
    private MongoSeataRmHelper mongoSeataRMHelper;

    public ProxyMongoTransactionManager(MongoDatabaseFactory dbFactory) {
        super(dbFactory);
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        Object transaction = super.doGetTransaction();
        mongoSeataRMHelper.doStartAndBindContext();
        return transaction;

    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        super.doRollback(status);

        mongoSeataRMHelper.doFailure();
    }

    @Override
    protected void prepareForCommit(DefaultTransactionStatus status) {
        super.prepareForCommit(status);

        mongoSeataRMHelper.doCommit();
    }

    @Override
    protected void doCommit(MongoTransactionObject transactionObject) throws Exception {
        super.doCommit(transactionObject);

        mongoSeataRMHelper.doSuccess();
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        super.doCleanupAfterCompletion(transaction);
        mongoSeataRMHelper.doCleanAfterCompletion();
    }
}

package io.seata.mongo.tx.support;

import io.seata.rm.datasource.ConnectionProxy.LockRetryPolicy;
import java.util.concurrent.Callable;

/**
 * @description: mongo遇到锁时的重试机制
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/19 14:34
 * @version:v1.0
 */
public class MongoLockRetryPolicy extends LockRetryPolicy {

    @Override
    public <T> T execute(Callable<T> callable) throws Exception {
        if (LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT) {
            return doRetryOnLockConflict(callable);
        } else {
            return callable.call();
        }
    }

    @Override
    protected void onException(Exception e) throws Exception {
        // 获取锁失败时重新生成前后镜像
        // 需要在datasource处进行，暂不处理，要求用户重新操作
    }
}

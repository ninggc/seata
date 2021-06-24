package io.seata.mongo.tx.mongo.util;

import io.seata.mongo.tx.mongo.bo.CustomUndoRecordBO;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @description: 发生脏写时的执行策略
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/18 14:45
 * @version:v1.0
 */
@Slf4j
@Service
public class DirtyWriteStrategy {

    private static final String TAG = "DIRTY_WRITE >> ";

    /**
     * mongo数据回滚失败，记录信息
     * 当前出现脏写只记录日志，不抛出异常
     *
     * @param xid
     * @param branchId
     * @param dbData
     * @param bos
     */
    public void consume(String xid, long branchId, Object dbData, List<CustomUndoRecordBO> bos) {
        String msg = String.format(TAG + "数据出现脏写，该记录将不会回滚: xid = %s, branchId = %s, po = %s, bos = %s"
            , xid, branchId, dbData, bos);

        throw new DirtyWriteException(xid, branchId, msg);
    }
}

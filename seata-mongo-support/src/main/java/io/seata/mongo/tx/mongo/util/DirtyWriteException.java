package io.seata.mongo.tx.mongo.util;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @description: 脏写时抛出的异常
 * @author: ninggc
 * @modified By: ninggc
 * @date: Created in 2021/5/24 15:16
 * @version:v1.0
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DirtyWriteException extends RuntimeException {
    private String xid;
    private Long branchId;

    public DirtyWriteException(String xid, Long branchId, String msg) {
        super(msg);
        this.xid = xid;
        this.branchId = branchId;
    }

}

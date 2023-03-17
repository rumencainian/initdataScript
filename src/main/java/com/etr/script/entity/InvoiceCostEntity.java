package com.etr.script.entity;


import lombok.Data;


/**
 * 发票相关费用
 *
 * @author lgq
 * @date 2023-3-7
 */
@Data
public class InvoiceCostEntity {

    /**
     * 开票id
     */
    private String applicationId;
    /**
     * 用户id
     */
    private String userId;

    /**
     * 花费金额
     */
    private Long costCount;
    /**
     * 花费类型
     * 1增值服务及技术服务费  2通行服务费  3延保服务费  4注销服务费
     */
    private Integer costType;

    /**
     * 支付时间
     */
    private String payTime;


    /**
     * 交易流水号
     */
    private String tradeNo;
    /**
     * 开票状态 0:待开票  1：开票中 2：已开票  9：退费
     */
    private Integer status;

    /**
     * 来源表
     */
    private String form;
    /**
     * version
     * 版本号
     */
    private String version;

}

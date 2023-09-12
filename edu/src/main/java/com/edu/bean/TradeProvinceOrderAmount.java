package com.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    BigDecimal orderAmount;
    // 下单人数
    BigDecimal orderUserCt;
    // 下单次数
    BigDecimal orderCt;
}
package com.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TradeScOrder
 * Package: com.edu.bean
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 10:24
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeScOrder {

	String sc;

	Integer ordered;
}

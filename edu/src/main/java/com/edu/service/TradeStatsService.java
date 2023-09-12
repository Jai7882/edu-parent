package com.edu.service;

import com.edu.bean.TradeProvinceOrderAmount;
import com.edu.bean.TradeScAmount;
import com.edu.bean.TradeScOrder;
import com.edu.bean.TradeScTran;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TradeStatsService
 * Package: com.edu.service
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 10:08
 * @Version 1.0
 */

public interface TradeStatsService {
	List<TradeScAmount> getScAmount(Integer date, Integer limit);

	List<TradeScOrder> getScOrder(Integer date, Integer limit);

	List<TradeScTran> getScTran(Integer date, Integer limit);

	List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}

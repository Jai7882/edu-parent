package com.edu.service.impl;

import com.edu.bean.TradeProvinceOrderAmount;
import com.edu.bean.TradeScAmount;
import com.edu.bean.TradeScOrder;
import com.edu.bean.TradeScTran;
import com.edu.mapper.TradeStatsMapper;
import com.edu.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TradeStatsServiceImpl
 * Package: com.edu.service.impl
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 10:08
 * @Version 1.0
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {

	@Autowired
	private TradeStatsMapper tradeStatsMapper;
	@Override
	public List<TradeScAmount> getScAmount(Integer date, Integer limit) {
		List<TradeScAmount> result  = tradeStatsMapper.selectScAmount(date,limit);

		return result;
	}

	@Override
	public List<TradeScOrder> getScOrder(Integer date, Integer limit) {
		List<TradeScOrder> result  = tradeStatsMapper.selectScOrder(date,limit);

		return result;
	}

	@Override
	public List<TradeScTran> getScTran(Integer date, Integer limit) {
		List<TradeScTran> result  = tradeStatsMapper.selectScTran(date,limit);

		return result;
	}

	@Override
	public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
		return tradeStatsMapper.selectProvinceAmount(date);
	}
}

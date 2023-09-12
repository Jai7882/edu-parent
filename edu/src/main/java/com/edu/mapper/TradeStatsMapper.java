package com.edu.mapper;

import com.edu.bean.TradeProvinceOrderAmount;
import com.edu.bean.TradeScAmount;
import com.edu.bean.TradeScOrder;
import com.edu.bean.TradeScTran;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * ClassName: TradeStatsMapper
 * Package: com.edu.mapper
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 10:12
 * @Version 1.0
 */
@Mapper
public interface TradeStatsMapper {

	@Select("select source_name sc , sum(order_amount) amount from dws_trade_sc_ar_window " +
			"Partition(par#{date})  group by source_name order by amount desc limit #{limit}")
	List<TradeScAmount> selectScAmount(Integer date, Integer limit);

	@Select("select source_name sc , sum(order_ct) ordered from dws_trade_sc_ar_window " +
			"Partition(par#{date})  group by source_name order by ordered desc limit #{limit}")
	List<TradeScOrder> selectScOrder(Integer date, Integer limit);

	@Select("select source_name sc , sum(order_ct) ordered from dws_trade_sc_ar_window " +
			"Partition(par#{date})  group by source_name order by ordered desc limit #{limit}")
	List<TradeScTran> selectScTran(Integer date, Integer limit);

	//获取某天各个省份交易额
	@Select("select province_name,sum(order_amount) order_amount,sum(order_user_ct) order_user_ct," +
			" sum(order_ct) order_ct from dws_trade_sc_ar_window PARTITION (par#{date}) group by province_name")
	List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}

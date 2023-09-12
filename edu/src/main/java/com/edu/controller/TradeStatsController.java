package com.edu.controller;

import com.edu.bean.TradeProvinceOrderAmount;
import com.edu.bean.TradeScAmount;
import com.edu.bean.TradeScOrder;
import com.edu.bean.TradeScTran;
import com.edu.service.TradeStatsService;
import com.edu.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: TradeStatsController
 * Package: com.edu.controller
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 10:07
 * @Version 1.0
 */
@RestController
public class TradeStatsController {

	@Autowired
	TradeStatsService tradeStatsService;

	@RequestMapping("/sc_amount")
	public String getScUv(@RequestParam(value = "date", defaultValue = "0") Integer date,
						  @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
		if (date == 0){
			date = DateFormatUtil.now();
		}

		List<TradeScAmount> result = tradeStatsService.getScAmount(date,limit);
		List<Double> amount = new ArrayList<>();
		List<String> sc = new ArrayList<>();
		for (TradeScAmount tradeScAmount : result) {
			amount.add(tradeScAmount.getAmount());
			sc.add(tradeScAmount.getSc());
		}

		String json = "{\n" +
				"  \"status\": 0,\n" +
				"  \"data\": {\n" +
				"    \"categories\": [\"" + StringUtils.join(sc, "\",\"") + "\"],\n" +
				"    \"series\": [\n" +
				"      {\n" +
				"        \"name\": \"销售额\",\n" +
				"        \"data\": ["+StringUtils.join(amount,",")+"]\n" +
				"      }\n" +
				"    ]\n" +
				"  }\n" +
				"}";
		return json;
	}

	@RequestMapping("/sc_order")
	public String getScOrder(@RequestParam(value = "date", defaultValue = "0") Integer date,
						  @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
		if (date == 0){
			date = DateFormatUtil.now();
		}

		List<TradeScOrder> result = tradeStatsService.getScOrder(date,limit);
		List<Integer> amount = new ArrayList<>();
		List<String> sc = new ArrayList<>();
		for (TradeScOrder tradeScOrder : result) {
			amount.add(tradeScOrder.getOrdered());
			sc.add(tradeScOrder.getSc());
		}

		String json = "{\n" +
				"  \"status\": 0,\n" +
				"  \"data\": {\n" +
				"    \"categories\": [\"" + StringUtils.join(sc, "\",\"") + "\"],\n" +
				"    \"series\": [\n" +
				"      {\n" +
				"        \"name\": \"下单数\",\n" +
				"        \"data\": ["+StringUtils.join(amount,",")+"]\n" +
				"      }\n" +
				"    ]\n" +
				"  }\n" +
				"}";

		return json;
	}

	@RequestMapping("/sc_tran")
	public String getScTran(@RequestParam(value = "date", defaultValue = "0") Integer date,
							 @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
		if (date == 0){
			date = DateFormatUtil.now();
		}

		List<TradeScTran> result = tradeStatsService.getScTran(date,limit);
		List<Double> trans = new ArrayList<>();
		List<String> sc = new ArrayList<>();
		for (TradeScTran tran : result) {
			trans.add(tran.getTran());
			sc.add(tran.getSc());
		}

		String json = "{\n" +
				"  \"status\": 0,\n" +
				"  \"data\": {\n" +
				"    \"categories\": [\"" + StringUtils.join(sc, "\",\"") + "\"],\n" +
				"    \"series\": [\n" +
				"      {\n" +
				"        \"name\": \"下单数\",\n" +
				"        \"data\": ["+StringUtils.join(trans,",")+"]\n" +
				"      }\n" +
				"    ]\n" +
				"  }\n" +
				"}";

		return json;
	}

	@RequestMapping("/province")
	public Map getProvinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0) {
			date = DateFormatUtil.now();
		}
		List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);
		Map resMap = new HashMap();
		resMap.put("status",0);

		Map dataMap = new HashMap();
		List dataList = new ArrayList();

		List<String> toolTipName = new ArrayList<>();
		List<String> toolTipUnits = new ArrayList<>();
		for (TradeProvinceOrderAmount provinceOrderAmount : provinceOrderAmountList) {
			List<BigDecimal> toolTip = new ArrayList<>();
			Map map = new HashMap();
			map.put("name",provinceOrderAmount.getProvinceName());
			map.put("value",provinceOrderAmount.getOrderAmount());

			toolTip.add(provinceOrderAmount.getOrderCt());
			toolTip.add(provinceOrderAmount.getOrderUserCt());
			map.put("tooltipValues",toolTip);

			dataList.add(map);
		}
		toolTipName.add("下单次数");
		toolTipName.add("下单人数");
		toolTipUnits.add("次");
		toolTipUnits.add("人");
		dataMap.put("mapData",dataList);
		dataMap.put("valueName","交易额");
		dataMap.put("tooltipNames",toolTipName);
		dataMap.put("tooltipUnits",toolTipUnits);
		resMap.put("data",dataMap);
		return resMap;
	}
}

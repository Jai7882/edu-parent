package com.edu.controller;

import com.edu.bean.TrafficScJump;
import com.edu.bean.TrafficScUvCt;
import com.edu.service.TrafficStatsService;
import com.edu.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: TrafficStatsController
 * Package: com.edu.controller
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 8:42
 * @Version 1.0
 */
@RestController
public class TrafficStatsController {

	@Autowired
	TrafficStatsService trafficStatsService;

	@RequestMapping("/sc_uv")
	public String getScUv(@RequestParam(value = "date", defaultValue = "0") Integer date,
						  @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
		if (date == 0){
			date = DateFormatUtil.now();
		}
		List<TrafficScUvCt> uvCt = trafficStatsService.getScUv(date,limit);
		List<Integer> uv = new ArrayList<>();
		List<String> sc = new ArrayList<>();
		for (TrafficScUvCt trafficScUvCt : uvCt) {
			uv.add(trafficScUvCt.getUv());
			sc.add(trafficScUvCt.getSource_name());
		}
		String json = "{\n" +
				"  \"status\": 0,\n" +
				"  \"data\": {\n" +
				"    \"categories\": [\"" + StringUtils.join(sc, "\",\"") + "\"],\n" +
				"    \"series\": [\n" +
				"      {\n" +
				"        \"name\": \"独立访客数\",\n" +
				"        \"data\": ["+StringUtils.join(uv,",")+"]\n" +
				"      }\n" +
				"    ]\n" +
				"  }\n" +
				"}";
		return json;
	}


	@RequestMapping("/sc_jump")
	public String getScJump(@RequestParam(value = "date", defaultValue = "0") Integer date,
						  @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
		if (date == 0){
			date = DateFormatUtil.now();
		}
		List<TrafficScJump> uvCt = trafficStatsService.getScJump(date,limit);
		List<Double> jump = new ArrayList<>();
		List<String> sc = new ArrayList<>();
		for (TrafficScJump trafficScUvCt : uvCt) {
			jump.add(trafficScUvCt.getJump_out());
			sc.add(trafficScUvCt.getSource_name());
		}
		String json = "{\n" +
				"  \"status\": 0,\n" +
				"  \"data\": {\n" +
				"    \"categories\": [\"" + StringUtils.join(sc, "\",\"") + "\"],\n" +
				"    \"series\": [\n" +
				"      {\n" +
				"        \"name\": \"跳出率\",\n" +
				"        \"data\": ["+StringUtils.join(jump,",")+"]\n" +
				"      }\n" +
				"    ]\n" +
				"  }\n" +
				"}";
		return json;
	}

}

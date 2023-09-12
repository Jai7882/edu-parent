package com.edu.service.impl;

import com.edu.bean.TrafficScJump;
import com.edu.bean.TrafficScUvCt;
import com.edu.mapper.TrafficStatsMapper;
import com.edu.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TrafficServiceImpl
 * Package: com.edu.service.impl
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 8:50
 * @Version 1.0
 */
@Service
public class TrafficServiceImpl implements TrafficStatsService {

	@Autowired
	private TrafficStatsMapper trafficStatsMapper;

	@Override
	public List<TrafficScUvCt> getScUv(Integer date, Integer limit) {
		List<TrafficScUvCt> result = trafficStatsMapper.getSvUv(date,limit);
		System.out.println(result);
		return result;
	}

	@Override
	public List<TrafficScJump> getScJump(Integer date, Integer limit) {
		List<TrafficScJump> result = trafficStatsMapper.getSvJump(date,limit);
		System.out.println(result);
		return result;
	}
}

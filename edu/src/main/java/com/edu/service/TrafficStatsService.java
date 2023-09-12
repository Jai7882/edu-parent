package com.edu.service;

import com.edu.bean.TrafficScJump;
import com.edu.bean.TrafficScUvCt;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ClassName: TrafficStatsService
 * Package: com.edu.service
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 8:49
 * @Version 1.0
 */
@Service
public interface TrafficStatsService {
	List<TrafficScUvCt> getScUv(Integer date, Integer limit);

	List<TrafficScJump> getScJump(Integer date, Integer limit);
}

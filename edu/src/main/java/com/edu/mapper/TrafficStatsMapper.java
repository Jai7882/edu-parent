package com.edu.mapper;

import com.edu.bean.TrafficScJump;
import com.edu.bean.TrafficScUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * ClassName: TrafficStatsMapper
 * Package: com.edu.mapper
 * Description:
 *
 * @Author jjy
 * @Create 2023/9/11 8:52
 * @Version 1.0
 */
@Mapper
public interface TrafficStatsMapper {
	@Select("select source_name , sum(cast(uv_ct as int)) uv from dws_traffic_sc_is_new_window Partition(par#{date}) group by source_name order by uv desc limit #{limit}")
	List<TrafficScUvCt> getSvUv(@Param("date") Integer date , @Param("limit") Integer limit);

	@Select("select source_name , cast(nvl(sum(so_ct)/sum(sv_ct),0)as decimal(16,2)) jump_out from dws_traffic_sc_is_new_window " +
			"Partition(par#{date}) group by source_name order by jump_out desc limit #{limit}")
	List<TrafficScJump> getSvJump(@Param("date") Integer date , @Param("limit") Integer limit);
}

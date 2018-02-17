package tech.debs17gc.system.time;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TimeStampWatermark implements AssignerWithPeriodicWatermarks<Tuple4<Integer, Integer, Integer, Double>>{

	@Override
	public long extractTimestamp(Tuple4<Integer, Integer, Integer, Double> element, long previousElementTimestamp) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// TODO Auto-generated method stub
		return null;
	}

}

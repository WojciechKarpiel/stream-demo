package pl.edu.agh;

import org.apache.flink.api.common.functions.MapFunction;

public interface Algorithm extends MapFunction<String, String> {
    boolean checkName(String name);
}

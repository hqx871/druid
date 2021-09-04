package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.scheduling.ArbitraryResourceScheduleStrategy;
import org.apache.druid.server.scheduling.LaningResourceScheduleStrategy;

import java.nio.ByteBuffer;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "strategy", defaultImpl = ArbitraryResourceScheduleStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "arbitrary", value = ArbitraryResourceScheduleStrategy.class),
    @JsonSubTypes.Type(name = "lane", value = LaningResourceScheduleStrategy.class)
})
public interface ResourceScheduleStrategy
{
  BlockingPool<ByteBuffer> getMergeBufferPool(
      ServerConfig serverConfig,
      QuerySchedulerConfig querySchedulerConfig,
      DruidProcessingConfig config
  );
}

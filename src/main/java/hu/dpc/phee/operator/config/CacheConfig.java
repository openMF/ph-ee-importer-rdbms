package hu.dpc.phee.operator.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableCaching
@EnableScheduling
public class CacheConfig {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Bean
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager("tenantServerConnection");
    }

    @CacheEvict(allEntries = true, value = {"tenantServerConnection"})
    @Scheduled(fixedDelay = 10 * 60 * 1000, initialDelay = 500)
    public void reportCacheEvict() {
        logger.info("Flushing all cached tenantServerConnection entries");
    }
}

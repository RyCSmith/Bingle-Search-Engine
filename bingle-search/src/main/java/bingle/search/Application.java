package bingle.search;

import net.sf.ehcache.config.CacheConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.context.annotation.Bean;

/**
 * Driver Class to launch the search engine web app.
 *
 */
@EnableAutoConfiguration
@SpringBootApplication
@EnableCaching
public class Application {
	
	/**
	 * Driver for the SearchEngine Web App.
	 * @param args
	 */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
    
    /**
     * Creates an EHCache for the web app. Creates three distinct caches for different operations.
     * @return - an ehcache.CacheManager
     */
    @Bean(destroyMethod="shutdown")
    public net.sf.ehcache.CacheManager ehCacheManager() {
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName("tfidfCache");
        cacheConfiguration.setMemoryStoreEvictionPolicy("LRU");
        cacheConfiguration.setMaxEntriesLocalHeap(1000);

        CacheConfiguration cacheConfigurationPR = new CacheConfiguration();
        cacheConfigurationPR.setName("prCache");
        cacheConfigurationPR.setMemoryStoreEvictionPolicy("LRU");
        cacheConfigurationPR.setMaxEntriesLocalHeap(1000);
        
        CacheConfiguration cacheConfigurationRes = new CacheConfiguration();
        cacheConfigurationRes.setName("resultsCache");
        cacheConfigurationRes.setMemoryStoreEvictionPolicy("LRU");
        cacheConfigurationRes.setMaxEntriesLocalHeap(1000);
        
        net.sf.ehcache.config.Configuration config = new net.sf.ehcache.config.Configuration();
        config.addCache(cacheConfiguration);
        config.addCache(cacheConfigurationPR);
        config.addCache(cacheConfigurationRes);

        return net.sf.ehcache.CacheManager.newInstance(config);
    }

    /**
     * Creates a CacheManager to be used for the web app.
     * @return springframework.cache.CacheManager.
     */
    @Bean
    public CacheManager cacheManager() {
        return new EhCacheCacheManager(ehCacheManager());
    }

}
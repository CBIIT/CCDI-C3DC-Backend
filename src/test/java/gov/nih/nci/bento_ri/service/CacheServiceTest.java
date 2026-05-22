package gov.nih.nci.bento_ri.service;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CacheServiceTest {

    @Test
    @DisplayName("caffeineCache creates a usable cache instance")
    void caffeineCache_shouldCreateUsableCache() {
        CacheService cacheService = new CacheService();

        Cache<String, Object> cache = cacheService.caffeineCache();
        cache.put("k", "v");

        assertThat(cache).isNotNull();
        assertThat(cache.getIfPresent("k")).isEqualTo("v");
    }
}

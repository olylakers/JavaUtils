package cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalCacheImpl implements ICache<String, Object> {
	private static final Log Logger = LogFactory.getLog(LocalCacheImpl.class);

	/**
	 * 具体内容存放的地方
	 */
	ConcurrentHashMap<String, Object>[] caches;
	/**
	 * 超期信息存储
	 */
	ConcurrentHashMap<String, Long> expiryCache;

	/**
	 * 清理超期内容的服务
	 */
	private ScheduledExecutorService scheduleService;

	/**
	 * 清理超期信息的时间间隔，默认20分钟
	 */
	private int expiryInterval = 20;

	/**
	 * 内部cache的个数，根据key的hash对module取模来定位到具体的某一个内部的Map， 减小阻塞情况发生。
	 */
	private int moduleSize = 10;
	
	private int maxSize = 5000;
	
	private String cacheName = "default";
	
	/**
	 * 主动清理过期缓存时，随机挑选的设置了过期时间的缓存数量
	 */
	private int activeExpireLookupCount = 100;
	
	/**
	 * 主动清理缓存时，会随机地从设置了过期时间的缓存中挑选activeExpireLookupCount 个缓存元素
	 * 如果当中有低于该值的缓存已经失效，则停止主动失效，否则继续遍历
	 */
	private float stopTraverseRate = 0.25f;
	
	private volatile AtomicInteger totalCacheEntryCount = new AtomicInteger();

	public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public float getStopTraverseRate() {
        return stopTraverseRate;
    }

    public void setStopTraverseRate(float stopTraverseRate) {
        this.stopTraverseRate = stopTraverseRate;
    }

    public int getActiveExpireLookupCount() {
        return activeExpireLookupCount;
    }

    public void setActiveExpireLookupCount(int activeExpireLookupCount) {
        this.activeExpireLookupCount = activeExpireLookupCount;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public LocalCacheImpl() {
		init();
	}

	public LocalCacheImpl(int expiryInterval, int moduleSize, int maxSize, int activeExpireLookupCount, float stopTraverseRate,  String cacheName) {
		this.expiryInterval = expiryInterval;
		this.moduleSize = moduleSize;
		this.maxSize = maxSize;
		this.cacheName = cacheName;
		init();
	}

	@SuppressWarnings("unchecked")
	private void init() {
		caches = new ConcurrentHashMap[moduleSize];

		for (int i = 0; i < moduleSize; i++)
			caches[i] = new ConcurrentHashMap<String, Object>();

		expiryCache = new ConcurrentHashMap<String, Long>();

		scheduleService = Executors.newScheduledThreadPool(1);

		scheduleService.scheduleAtFixedRate(new CheckOutOfDateSchedule(caches,
				expiryCache), 0, expiryInterval * 60, TimeUnit.SECONDS);

        Logger.error("DefaultCache CheckService is start for "+cacheName);
	}

	public boolean clear() {
		if (caches != null)
			for (ConcurrentHashMap<String, Object> cache : caches) {
				cache.clear();
			}

		if (expiryCache != null)
			expiryCache.clear();

		return true;
	}

	public boolean containsKey(String key) {
		checkValidate(key);
		return getCache(key).containsKey(key);
	}

	public Object get(String key) {
		checkValidate(key);
		return getCache(key).get(key);
	}

	public Set<String> keySet() {
		checkAll();
		return expiryCache.keySet();
	}

	public Object put(String key, Object value) {
        return put(key, value, -1L);
	}

	public Object put(String key, Object value, Date expiry) {
        return put(key, value, expiry.getTime());
	}

	/**
	 * 如果缓存已超过最大容量，则返回null
	 * @param key
	 * @param value
	 * @param expiryTime
	 * @return
	 */
    public Object put(String key, Object value, long expiryTime) {
        int currentCount = totalCacheEntryCount.incrementAndGet();
        if(currentCount <= maxSize){
            Object result = getCache(key).put(key, value);
            if(expiryTime >0){
                expiryCache.put(key, expiryTime);
            }
            return result;            
        }else{
            totalCacheEntryCount.decrementAndGet();
            Logger.error(cacheName+ " is overlimit "+totalCacheEntryCount.get());
            return null;
        }
    }
	
	public Object remove(String key) {
		Object result = getCache(key).remove(key);
		expiryCache.remove(key);
		totalCacheEntryCount.decrementAndGet();
		return result;
	}

	public int size() {
		checkAll();

		return expiryCache.size();
	}

	public Collection<Object> values() {
		checkAll();

		Collection<Object> values = new ArrayList<Object>();

		for (ConcurrentHashMap<String, Object> cache : caches) {
			values.addAll(cache.values());
		}

		return values;
	}

	private ConcurrentHashMap<String, Object> getCache(String key) {
		long hashCode = (long) key.hashCode();

		if (hashCode < 0)
			hashCode = -hashCode;

		int moudleNum = (int) hashCode % moduleSize;

		return caches[moudleNum];
	}

	private void checkValidate(String key) {
		Long value = expiryCache.get(key);
		if (key != null && value != null && value != -1
				&& new Date(value).before(new Date())) {
			getCache(key).remove(key);
			expiryCache.remove(key);
		}
	}

	private void checkAll() {
		Iterator<String> iter = expiryCache.keySet().iterator();

		while (iter.hasNext()) {
			String key = iter.next();
			checkValidate(key);
		}
	}

	class CheckOutOfDateSchedule implements java.lang.Runnable {
		/**
		 * 具体内容存放的地方
		 */
		ConcurrentHashMap<String, Object>[] caches;
		/**
		 * 超期信息存储
		 */
		ConcurrentHashMap<String, Long> expiryCache;

		public CheckOutOfDateSchedule(
				ConcurrentHashMap<String, Object>[] caches,
				ConcurrentHashMap<String, Long> expiryCache) {
			this.caches = caches;
			this.expiryCache = expiryCache;
		}

		public void run() {
		    clearExpire();
		}
	}

	/**
	 * 随机从设置了失效时间的缓存中挑选出特定数量的缓存，如果这些被随机挑出来的缓存当中有超过一定比例的缓存已经失效，
	 * 则认为当前缓存中已有较多的缓存需要主动失效，则继续下一次随机挑选，否则终止主动失效
	 */
    public void clearExpire() {
        int expireTotalCount = 0;
        try {
            int totalExpireCount = 0;
            Random random = new Random();
            do {
                totalExpireCount = 0;
                long now = System.currentTimeMillis();
                Map<String, Long> selectMap = getRandomKeyFromMap(expiryCache, random, activeExpireLookupCount);
                if(selectMap!= null && selectMap.size() >0){
                    for (Entry<String, Long> entry : selectMap.entrySet()) {
                        if(entry.getValue() <= now){
                            expiryCache.remove(entry.getKey());
                            remove(entry.getKey());
                            totalExpireCount++;
                            expireTotalCount++;
                        }
                    }                    
                }
            } while (totalExpireCount > activeExpireLookupCount*stopTraverseRate);
            
            Logger.error(cacheName +" expireTotalCount :" +expireTotalCount+", totalCacheEntryCount: "+totalCacheEntryCount.get());
        } catch (Exception ex) {
            Logger.error("DefaultCache CheckService is error!"+ex.getMessage());
        }
    }
	
	public Object put(String key, Object value, int TTL) {
        return put(key, value, System.currentTimeMillis()+TTL*1000);
	}

	public void destroy() {
		try {
			clear();

			if (scheduleService != null)
				scheduleService.shutdown();

			scheduleService = null;
		} catch (Exception ex) {
			Logger.error(ex);
		}
	}
	
	/**
	 * 从map中随机挑选出number个元素
	 * @param expiryCache
	 * @param random
	 * @param getNumber
	 * @return
	 */
    private Map<String, Long> getRandomKeyFromMap(
            ConcurrentHashMap<String, Long> expiryCache, Random random,
            int getNumber) {
        Map<String, Long> selectMap = new HashMap<String, Long>(getNumber);
        int seen = 0;
        int n = expiryCache.size();
        for (Entry<String, Long> entrySet : expiryCache.entrySet()) {
            if (getNumber == 0)
                break;

            double prob = getNumber / (double) (n - seen);
            if (random.nextDouble() < prob) {
                selectMap.put(entrySet.getKey(),entrySet.getValue());
                getNumber--;
            }
            seen++;            
        }
        
        return selectMap;
    }
}
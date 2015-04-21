package me.bpulse.java.client.pulsesrepository;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.fusesource.lmdbjni.EntryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EhCachePulsesRepository implements IPulsesRepository {
	
	private CacheManager mgr;
	private CacheManager mgr2;
	private Configuration configuration;
	private Configuration configuration2;
	private Cache c;
	private Cache c2;
	private int insertedRecords = 0;
	private long insertTimeMillisAverage = 0;
	private long deleteTimeMillisAverage = 0;
	private long getTimeMillisAverage = 0;
	private long sortedKeysTimeMillisAverage = 0;
	private long initialLoadCacheTime = 0;
	private boolean firstShutdown = false;
	private Map<Object, Element> bpulseRQReaderMap;
	private Map<String,String> bpulseRQInProgressMap;
	private final static Logger logger = LoggerFactory.getLogger(EhCachePulsesRepository.class);
	
	public EhCachePulsesRepository() {
		String dbPath = PropertiesManager.getProperty("bpulse.client.pulsesRepositoryDBPath");
		if (dbPath == null) {
			//TODO DEFINIR PATH POR DEFECTO PARA CREAR DB
		}
		// Already created a configuration object ...
		configuration=new Configuration();
		  DiskStoreConfiguration diskStore=new DiskStoreConfiguration();
		  diskStore.setPath(dbPath);
		  configuration.addDiskStore(diskStore);
		  configuration.addDefaultCache(new CacheConfiguration());
		  configuration.setName("mgr");
		  
		  configuration2=new Configuration();
		  configuration2.addDefaultCache(new CacheConfiguration());
		  configuration2.setName("mgr2");
		 
		this.mgr = CacheManager.newInstance(configuration);// new CacheManager(configuration);
		c = new Cache("BPULSEEHDB", 1000000, MemoryStoreEvictionPolicy.LRU, true, dbPath, true, 0, 0, true, 0, null);
		mgr.addCache(c);
		initialLoadCacheTime = System.currentTimeMillis();
		this.mgr2 = CacheManager.newInstance(configuration2);//new CacheManager(configuration2);
		c2 = new Cache("BPULSEEHDBREAD", 1000000, MemoryStoreEvictionPolicy.LRU, false, null, true, 0, 0, false, 0, null);
		this.mgr2.addCache(c2);
		
		bpulseRQInProgressMap = new HashMap<String,String>();
		
		//VERIFY IF EXISTS PULSES SAVED IN THE SINGLETON BUILD
		bpulseRQReaderMap = new HashMap<Object, Element>();
		if (c.getSize() > 0) {
			
			c2 = this.mgr2.getCache("BPULSEEHDBREAD");
			bpulseRQReaderMap = getCurrentPulses(c);
			c2.putAll(bpulseRQReaderMap.values());
			insertedRecords = c.getSize();
			initialLoadCacheTime = System.currentTimeMillis();
			System.out.println("TAMANO READER: " + bpulseRQReaderMap.size());
		}
		
	}
	
	private Map<Object, Element> getCurrentPulses(Cache c) {
		List currentKeys = c.getKeys();
		return c.getAll(currentKeys);
	}


	public synchronized void savePulse(PulsesRQ pPulsesRQ) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		//this.mgr = CacheManager.newInstance(configuration);
		if(mgr.getStatus().equals(Status.STATUS_SHUTDOWN)) {
			this.mgr = CacheManager.newInstance(configuration);
		}
		Cache c = mgr.getCache("BPULSEEHDB");
		String key = "BPULSE-"+System.currentTimeMillis()+"-"+additionalPulseId;
		Element e = new Element(key, pPulsesRQ);
		c.put(e);
		c.flush();
		//Cache c2 = mgr2.getCache("BPULSEEHDBREAD");
		c2.put(e);
		//if (initialLoadCacheTime - initTime > 5000) {
		if (!firstShutdown) {
			mgr.shutdown();
			firstShutdown = true;
		}
		//mgr.shutdown();
		//bpulseRQReaderMap.put(key, e);
		insertedRecords++;
		this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		System.out.println("INSERTED RECORDS: " + insertedRecords + " " + Calendar.getInstance().getTime() + " INSERT AVERAGE MILLIS: " + insertTimeMillisAverage);
	}
	
	public synchronized Object[] getSortedbpulseRQMapKeys() {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		//this.mgr = CacheManager.newInstance(configuration);
		//Cache c = mgr2.getCache("BPULSEEHDBREAD");
		Object[] keys = c2.getKeys().toArray();
		//mgr.shutdown();
		this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	    //Arrays.sort(keys);
	    //trx.close();
	    return keys;
	}
	
	public synchronized PulsesRQ getBpulseRQByKey(String pKey) {
		PulsesRQ resp;
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		/*if (mgr.getStatus().equals(Status.STATUS_SHUTDOWN)) {
			this.mgr = CacheManager.newInstance(configuration);
		}
		Cache c = mgr.getCache("BPULSEEHDB");*/
		//Cache c = mgr2.getCache("BPULSEEHDBREAD");
		resp = (PulsesRQ) c2.get(pKey).getObjectValue();
		//mgr.shutdown();
		this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		//trx.close();
		return resp;
	}
	
	public synchronized void deleteBpulseRQByKey(String pKey) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		//this.mgr = CacheManager.newInstance(configuration);
		Cache c = mgr.getCache("BPULSEEHDB");
		c.remove(pKey);
		c.flush();
		//Cache c2 = mgr2.getCache("BPULSEEHDBREAD");
		c2.remove(pKey);
		//mgr.shutdown();
		//bpulseRQReaderMap.remove(pKey);
		this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	}
	
	public synchronized int countBpulsesRQ() {
		int resp;
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		//this.mgr = CacheManager.newInstance(configuration);
		//Cache c = mgr2.getCache("BPULSEEHDBREAD");
		resp = c2.getSize();
		//mgr.shutdown();
		//trx.close();
		return resp;
	}
	
	public synchronized void markBpulseKeyInProgress(String pKey) {
		bpulseRQInProgressMap.put(pKey, "INPROGRESS");//TODO CONSTANTE ESTADO INPROGRESS
		//processedPulsesDBInstance.commit();
	}
	
	public synchronized String getMarkedBPulseKeyInProgress(String pKey){
		return bpulseRQInProgressMap.get(pKey);
	}
	
	public int countMarkBpulseKeyInProgress() {
		return bpulseRQInProgressMap.size();
	}
	
	public synchronized void releaseBpulseKeyInProgressByKey(String pKey) {
		bpulseRQInProgressMap.remove(pKey);
	}
	
	public int getInsertedRecords() {
		return insertedRecords;
	}
	
	public long getInsertTimeMillisAverage() {
		return insertTimeMillisAverage;
	}
	
	public long getDeleteTimeMillisAverage() {
		return deleteTimeMillisAverage;
	}
	
	public long getGetTimeMillisAverage() {
		return getTimeMillisAverage;
	}
	
	public long getSortedKeysTimeMillisAverage() {
		return sortedKeysTimeMillisAverage;
	}

	@Override
	public EntryIterator getIterableEntriesBpulseRQMapKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getSerializedBpulseRQByKey(String pKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void initTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endTransaction() {
		// TODO Auto-generated method stub
		
	}
	

}

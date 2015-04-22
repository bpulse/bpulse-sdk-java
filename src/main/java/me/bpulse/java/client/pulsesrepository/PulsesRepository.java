package me.bpulse.java.client.pulsesrepository;

import java.io.File;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;

import org.fusesource.lmdbjni.EntryIterator;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.TxMaker;

public class PulsesRepository implements IPulsesRepository{
	
	private DB pulsesDBInstance;
	private DB processedPulsesDBInstance;
	private ConcurrentNavigableMap<String,PulsesRQ> bpulseRQMap;
	private SortedMap<String,PulsesRQ> bpulseRQReaderMap;
	private HashMap<String,String> bpulseRQInProgressMap;
	private int insertedRecords = 0;
	private long insertTimeMillisAverage = 0;
	private long deleteTimeMillisAverage = 0;
	private long getTimeMillisAverage = 0;
	private long sortedKeysTimeMillisAverage = 0;
	
	public PulsesRepository() {
		String dbPath = PropertiesManager.getProperty("bpulse.client.pulsesRepositoryDBPath");
		if (dbPath == null) {
			//TODO DEFINIR PATH POR DEFECTO PARA CREAR DB
		}
		pulsesDBInstance = DBMaker.newFileDB(new File(dbPath))
				.transactionDisable()
				.mmapFileEnable()
                .closeOnJvmShutdown()
                .encryptionEnable("BPULSEDB")
                //.cacheSize(1000000)
                //.mmapFileEnableIfSupported()
                //.cacheLRUEnable()
                .make();
		
		processedPulsesDBInstance = DBMaker.newMemoryDB()
                .make();
		
		bpulseRQMap = pulsesDBInstance.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		/*pulsesDBInstance = DBMaker.newFileDB(new File(dbPath))
                .closeOnJvmShutdown()
                .encryptionEnable("BPULSEDB")
                .makeTxMaker();*/
		bpulseRQInProgressMap = new HashMap<String,String>();//processedPulsesDBInstance.getTreeMap("BPULSEPROCESSEDDB");//
		
		bpulseRQReaderMap = new TreeMap<String,PulsesRQ>();
		if (!bpulseRQMap.isEmpty()) {
			SortedMap<String,PulsesRQ> entry = getCurrentPulses(); 
			bpulseRQReaderMap = new TreeMap<String,PulsesRQ>(entry);
			insertedRecords = bpulseRQMap.size();
			System.out.println("TAMANO READER: " + bpulseRQReaderMap.size());
		}
		
	}
	
	private SortedMap<String, PulsesRQ> getCurrentPulses() {
		return bpulseRQMap.descendingMap();
	}

	public synchronized void savePulse(PulsesRQ pPulsesRQ) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		String key = "BPULSE-"+System.currentTimeMillis()+"-"+additionalPulseId;
		bpulseRQMap.put(key, pPulsesRQ);
		bpulseRQReaderMap.put(key, pPulsesRQ);
		//System.out.println("TAMANO NUEVO: " + bpulseRQMap.size());
		//trx.commit();
		//trx.close();
		insertedRecords++;
		pulsesDBInstance.commit();
		//System.out.println("TIEMPO INSERCION: " + (Calendar.getInstance().getTimeInMillis() - initTime));
		this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		System.out.println("INSERTED RECORDS: " + insertedRecords + " " + Calendar.getInstance().getTime() + " INSERT AVERAGE MILLIS: " + insertTimeMillisAverage);
	}
	
	public synchronized Object[] getSortedbpulseRQMapKeys() {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		Object[] keys = bpulseRQReaderMap.keySet().toArray();
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
		resp = bpulseRQReaderMap.get(pKey);
		this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		//trx.close();
		return resp;
	}
	
	public synchronized void deleteBpulseRQByKey(String pKey) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		bpulseRQMap.remove(pKey);
		bpulseRQReaderMap.remove(pKey);
		//System.out.println("TIEMPO BORRADO: " + (Calendar.getInstance().getTimeInMillis() - initTime));
		//System.out.println("TAMANO NUEVO: " + bpulseRQMap.size());
		//trx.commit();
		//trx.close();
		pulsesDBInstance.commit();
		this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	}
	
	public synchronized int countBpulsesRQ() {
		int resp;
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		resp = bpulseRQReaderMap.size();
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
	
	public void closeDBConnection() {
		pulsesDBInstance.close();
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

	@Override
	public PulsesRQ getBpulseRQByKey(Long pKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteBpulseRQByKey(Long pKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void markBpulseKeyInProgress(Long pKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseBpulseKeyInProgressByKey(Long pKey) {
		// TODO Auto-generated method stub
		
	}

}

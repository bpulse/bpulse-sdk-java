package me.bpulse.java.client.pulsesrepository;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;

import org.fusesource.lmdbjni.*;

import com.google.protobuf.InvalidProtocolBufferException;

import static org.fusesource.lmdbjni.Constants.*;

public class LMDBRepository implements IPulsesRepository {
	
	private int insertedRecords = 0;
	private int countRecords = 0;
	private long insertTimeMillisAverage = 0;
	private long deleteTimeMillisAverage = 0;
	private long getTimeMillisAverage = 0;
	private long sortedKeysTimeMillisAverage = 0;
	private HashMap<String,String> bpulseRQInProgressMap;
	private Env env;
	private Database db;
	private Transaction readTrx;
	
	public LMDBRepository() {
		try {
			String dbPath = PropertiesManager.getProperty("bpulse.client.pulsesRepositoryDBPath");
			if (dbPath == null) {
				//TODO DEFINIR PATH POR DEFECTO PARA CREAR DB
			}
			this.env = new Env(dbPath);
			this.env.setMapSize(1000000000);
			this.db = env.openDatabase("BPULSEDB");
			readTrx = env.createReadTransaction();
			bpulseRQInProgressMap = new HashMap<String,String>();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void savePulse(PulsesRQ pPulsesRQ) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		
		String key = "BPULSE-"+System.currentTimeMillis()+"-"+additionalPulseId;
		this.db.put(key.getBytes(), pPulsesRQ.toByteArray());
		insertedRecords++;
		countRecords++;
		this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	}
	
	public synchronized Object[] getSortedbpulseRQMapKeys() {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		
	    return null;
	}
	
	public synchronized EntryIterator getIterableEntriesBpulseRQMapKeys() {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		//Transaction trx = env.createReadTransaction();
		//readTrx = env.createReadTransaction();
		EntryIterator itEntry = this.db.iterate(readTrx);
		//trx.close();
		this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	    //Arrays.sort(keys);
	    //trx.close();
	    return itEntry;
	}
	
	public void initTransaction() {
		readTrx = env.createReadTransaction();
	}
	
	public void endTransaction() {
		readTrx.close();
	}
	
	public PulsesRQ getBpulseRQByKey(String pKey) {
		PulsesRQ resp;
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		//this.db = null;
		//this.db = env.openDatabase("BPULSEDB");
		Transaction trx = env.createReadTransaction();
		byte[] serializedPulse = this.db.get(trx, pKey.getBytes());
		trx.close();
		try {
			resp = PulsesRQ.parseFrom(serializedPulse);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			resp = null;
		}
		this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		//trx.close();
		return resp;
	}
	
	public byte[] getSerializedBpulseRQByKey(String pKey) {
		PulsesRQ resp;
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		//this.db = null;
		//this.db = env.openDatabase("BPULSEDB");
		byte[] serializedPulse = this.db.get(readTrx, pKey.getBytes());
		this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		//trx.close();
		return serializedPulse;
	}
	
	public synchronized void deleteBpulseRQByKey(String pKey) {
		//DB trx = pulsesDBInstance.makeTx();
		//bpulseRQMap = trx.getTreeMap("BPULSEDB");//TODO CONSTANTE NOMBRE DE DB PARA PULSOS
		long initTime = Calendar.getInstance().getTimeInMillis();
		Transaction trx = env.createWriteTransaction();
		this.db.delete(trx, pKey.getBytes());
		trx.commit();trx.close();
		countRecords--;
		this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
	}
	
	public synchronized int countBpulsesRQ() {
		return countRecords;
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

}

package me.bpulse.java.client.pulsesrepository;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.rowset.serial.SerialBlob;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REPOSITORY_NAME;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REPOSITORY_USER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_STATUS_INPROGRESS;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_STATUS_PENDING;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_1;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_2;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_3;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_5;

/**
 * @author Daniel Pineda - daniel.pineda@serhstourism.com
 */
public class InMemoryPulsesRepository implements IPulsesRepository {

	private int insertedRecords = COMMON_NUMBER_0;
	private long insertTimeMillisAverage = COMMON_NUMBER_0;
	private long deleteTimeMillisAverage = COMMON_NUMBER_0;
	private long getTimeMillisAverage = COMMON_NUMBER_0;
	private long sortedKeysTimeMillisAverage = COMMON_NUMBER_0;
	private int limitNumberPulsesToReadFromDb = COMMON_NUMBER_0;
	private Map<Long,PulsesRQ> bpulseRQInProgressMap;
	private Map<Long,PulsesRQ> bpulseRQMap = new HashMap<Long, PulsesRQ>();
	final static Logger logger = LoggerFactory.getLogger("bpulseLogger");
	
	public InMemoryPulsesRepository(int maxNumberPulsesToProcessByTimer) throws Exception {
		
		this.limitNumberPulsesToReadFromDb = maxNumberPulsesToProcessByTimer;
		
		bpulseRQInProgressMap = new HashMap<Long,PulsesRQ>();
		
		convertAllBpulseKeyInProgressToPending();
		
		
		
	}

	/**
	 * Method that saves the sent pulse in the PULSESDB.
	 * 
	 * @param pPulsesRQ The Pulse in Protobuf format.
	 */
	public synchronized void savePulse(PulsesRQ pPulsesRQ) throws Exception {
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		long key = System.currentTimeMillis()+additionalPulseId;
		
		bpulseRQMap.put(key, pPulsesRQ);
		
        insertedRecords++;
		this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);

	}

	/**
	 * Method that performs the query from all pulse keys pending to send to BPULSE REST SERVICE.
	 * 
	 * @return Object[] with all pulse keys found.
	 */
	public synchronized Object[] getSortedbpulseRQMapKeys() throws Exception{
		long initTime = Calendar.getInstance().getTimeInMillis();
		List<Object> resp = new ArrayList<Object>();
		try {
			List<Long> sortedKeys=new ArrayList<Long>(bpulseRQMap.keySet());
			Collections.sort(sortedKeys);
			
			int i=0;
			while (resp.size()<this.limitNumberPulsesToReadFromDb && i<sortedKeys.size()) {
				resp.add(sortedKeys.get(i));
				i++;
			}
			
			this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (Exception e) {
			logger.error("FAILED TO GET THE PENDING PULSES LIST: ", e);
			throw e;
		}
		
		
		return resp.toArray();
	}

	/**
	 * Method that obtains the associated pulsesRQ to the selected key from PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	public synchronized PulsesRQ getBpulseRQByKey(Long pKey) throws Exception{
		PulsesRQ resp = null;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
	        resp = bpulseRQMap.get(pKey);
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (Exception e) {
			logger.error("FAILED TO GET THE PULSE BY KEY: ", e);
			throw e;
		}
		return resp;
	}

	/**
	 * Method that performs the deletion of the associated pulsesRQ to the selected key.
	 * @param pKey The pulses key.
	 */
	public synchronized void deleteBpulseRQByKey(Long pKey) throws Exception{
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			bpulseRQMap.remove(pKey);
			bpulseRQInProgressMap.remove(pKey);
	        this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (Exception e) {
			logger.error("FAILED TO DELETE PULSE: ", e);
			throw e;
		}

	}

	/**
	 * Method that counts the current pulsesRQ in the PULSESDB.
	 * @return The pulses amount in the PULSESDB.
	 */
	public int countBpulsesRQ() throws Exception{
		int resp = COMMON_NUMBER_0;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			resp = bpulseRQMap.size();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (Exception e) {
			logger.error("FAILED TO GET THE CURRENT PULSES NUMBER IN DB: ", e);
			throw e;
		}
		
		return resp;
	}

	/**
	 * Method that performs the change of the pulsesRQ State from PENDING to INPROGRESS in the PULSESDB.
	 * @param pKey The pulses key.
	 */
	public synchronized void markBpulseKeyInProgress(Long pKey) throws Exception{
		
		try {
			bpulseRQInProgressMap.put(pKey, bpulseRQMap.get(pKey));
			
		} catch (Exception e) {
			logger.error("FAILED TO UPDATE THE PULSE STATE FROM PENDING TO INPROGRESS: ", e);
			throw e;
		}

	}

	/**
	 * Method that counts the number of pulses with PENDING state in the PULSESDB.
	 * @return The PENDING pulses amount.
	 */
	public int countMarkBpulseKeyInProgress() throws Exception{
		
		int resp = COMMON_NUMBER_0;
		try {
			
			resp = bpulseRQInProgressMap.size();
			
		} catch (Exception e) {
			logger.error("FAILED TO GET PULSES INPROGRESS COUNT: ", e);
			throw e;
		}
		
		return resp;
		
	}

	/**
	 * Method that performs the change of the pulsesRQ State from INPROGRESS to PENDING in the PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	public synchronized void releaseBpulseKeyInProgressByKey(Long pKey) throws Exception{
		
		try {
			
			bpulseRQInProgressMap.remove(pKey);
			
		} catch (Exception e) {
			logger.error("FAILED TO UPDATE THE PULSE STATE FROM INPROGRESS TO PENDING: ", e);
			throw e;
		}

	}
	
	/**
	 * Method that performs the change of the pulsesRQ State from INPROGRESS to PENDING for all the pulses in the PULSESDB.
	 */
	public void convertAllBpulseKeyInProgressToPending() throws Exception{
		
		try {
			bpulseRQInProgressMap.clear();
			
		} catch (Exception e) {
			logger.error("FAILED TO MASSIVE UPDATE THE PULSES STATE FROM INPROGRESS TO PENDING: ", e);
			throw e;
		}

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
	
	/**
	 * Method that gets the current size of the PULSESDB file.
	 * @return The PULSESDB file size in bytes.
	 */
	
	public long getDBSize() {
        return bpulseRQMap.size();
	}

	@Override
	public void savePulse(PulsesRQ pPulsesRQ, int tableIndex) throws Exception {
		savePulse(pPulsesRQ);
		
	}

	@Override
	public Object[] getSortedbpulseRQMapKeys(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int countBpulsesRQ(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int countMarkBpulseTableInProgress() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public PulsesRQ getBpulseRQByKey(Long pKey, int tableIndex)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void truncateBpulseRQByTableIndex(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void markBpulseTableInProgress(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseBpulseTableInProgress(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isAvailableBpulseTable(int tableIndex) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

}

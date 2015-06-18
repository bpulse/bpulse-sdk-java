/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.pulsesrepository;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public interface IPulsesRepository {
	
	public void savePulse(PulsesRQ pPulsesRQ) throws Exception;
	
	public void savePulse(PulsesRQ pPulsesRQ, int tableIndex) throws Exception;
	
	public Object[] getSortedbpulseRQMapKeys() throws Exception;
	
	public Object[] getSortedbpulseRQMapKeys(int tableIndex) throws Exception;
	
	public int countBpulsesRQ() throws Exception;
	
	public int countBpulsesRQ(int tableIndex) throws Exception;
	
	public int countMarkBpulseKeyInProgress() throws Exception;
	
	int countMarkBpulseTableInProgress() throws Exception;
	
	public int getInsertedRecords();
	
	public long getInsertTimeMillisAverage();
	
	public long getDeleteTimeMillisAverage();
	
	public long getGetTimeMillisAverage();
	
	public long getSortedKeysTimeMillisAverage();
	
	PulsesRQ getBpulseRQByKey(Long pKey) throws Exception;
	
	PulsesRQ getBpulseRQByKey(Long pKey, int tableIndex) throws Exception;

	void deleteBpulseRQByKey(Long pKey) throws Exception;
	
	void truncateBpulseRQByTableIndex(int tableIndex) throws Exception;

	void markBpulseKeyInProgress(Long pKey) throws Exception;
	
	void markBpulseTableInProgress(int tableIndex) throws Exception;

	void releaseBpulseKeyInProgressByKey(Long pKey) throws Exception;
	
	void releaseBpulseTableInProgress(int tableIndex) throws Exception;
	
	boolean isAvailableBpulseTable(int tableIndex) throws Exception;
	
	public long getDBSize();

}

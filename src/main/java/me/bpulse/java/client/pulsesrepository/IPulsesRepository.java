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
	
	public Object[] getSortedbpulseRQMapKeys() throws Exception;
	
	public int countBpulsesRQ() throws Exception;
	
	public int countMarkBpulseKeyInProgress() throws Exception;
	
	public int getInsertedRecords();
	
	public long getInsertTimeMillisAverage();
	
	public long getDeleteTimeMillisAverage();
	
	public long getGetTimeMillisAverage();
	
	public long getSortedKeysTimeMillisAverage();
	
	PulsesRQ getBpulseRQByKey(Long pKey) throws Exception;

	void deleteBpulseRQByKey(Long pKey) throws Exception;

	void markBpulseKeyInProgress(Long pKey) throws Exception;

	void releaseBpulseKeyInProgressByKey(Long pKey) throws Exception;
	
	public long getDBSize();

}

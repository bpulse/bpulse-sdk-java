package me.bpulse.java.client.pulsesrepository;

import org.fusesource.lmdbjni.EntryIterator;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;

public interface IPulsesRepository {
	
	public void savePulse(PulsesRQ pPulsesRQ);
	
	public Object[] getSortedbpulseRQMapKeys();
	
	public EntryIterator getIterableEntriesBpulseRQMapKeys();
	
	public PulsesRQ getBpulseRQByKey(String pKey);
	
	public byte[] getSerializedBpulseRQByKey(String pKey);
	
	public void deleteBpulseRQByKey(String pKey);
	
	public int countBpulsesRQ();
	
	public void markBpulseKeyInProgress(String pKey);
	
	public String getMarkedBPulseKeyInProgress(String pKey);
	
	public int countMarkBpulseKeyInProgress();
	
	public void releaseBpulseKeyInProgressByKey(String pKey);
	
	public int getInsertedRecords();
	
	public long getInsertTimeMillisAverage();
	
	public long getDeleteTimeMillisAverage();
	
	public long getGetTimeMillisAverage();
	
	public long getSortedKeysTimeMillisAverage();
	
	public void initTransaction();
	
	public void endTransaction();

	PulsesRQ getBpulseRQByKey(Long pKey);

	void deleteBpulseRQByKey(Long pKey);

	void markBpulseKeyInProgress(Long pKey);

	void releaseBpulseKeyInProgressByKey(Long pKey);

}

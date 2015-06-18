/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.timer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimerTask;


//import org.apache.log4j.Logger;
import org.slf4j.*;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;

import com.google.protobuf.InvalidProtocolBufferException;

import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_5;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_MINUS_5;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_MEM_PULSES_REPOSITORY;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_DB_PULSES_REPOSITORY;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseRestSenderTimer extends TimerTask{
	
	private BPulseSender bpulseSender;
	private String dbMode;
	//final static Logger logger = Logger.getLogger(BPulseRestSenderTimer.class);
	final static Logger logger = LoggerFactory.getLogger("bpulseLogger");
	
	public BPulseRestSenderTimer(BPulseSender pBpulseSender, String dbMode) {
		this.bpulseSender = pBpulseSender;
		this.dbMode = dbMode;
	}
	
	@Override
	public void run() {
		
		if(this.dbMode.equals(BPULSE_MEM_PULSES_REPOSITORY)) {
			executeRestPulsesSending();
		} else if(this.dbMode.equals(BPULSE_DB_PULSES_REPOSITORY)) {
			executePartitioningRestPulsesSending();
		}
		
	}
	
	/**
	 * Method that performs the pulses processing. It reads all the pending pulses from PULSESDB, for each of them, it marks as INPROGRESS and extracts the pulses list and add it to the summarizedPulsesRQToSend object.
	 * The summarizedPulsesRQToSend object will be sent to BPULSE REST SERVICE when it's pulses amount become equals to DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES.
	 * 
	 */
	private void executeRestPulsesSending() {
		
		try {
			//obtain the current key list
			IPulsesRepository pulsesRepository = bpulseSender.getPulsesRepository();
	        Object[] keys = pulsesRepository.getSortedbpulseRQMapKeys();
	        List<Long> keyPulseListToDelete = new ArrayList<Long>();
	        
	        PulsesRQ summarizedPulsesRQToSend = null;
	        PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
	        int totalOfPulsesToSend = COMMON_NUMBER_0;
	        int totalOfProcessedPulses = COMMON_NUMBER_0;
	        long summarizedTime = COMMON_NUMBER_0;
	        long init = COMMON_NUMBER_0;
	        long initGets = COMMON_NUMBER_0;
	        long summarizeGets = COMMON_NUMBER_0;
	        logger.info("BEGIN TIMER PULSES PROCESSING..." + " RECORDS READ FROM DB: " + pulsesRepository.countBpulsesRQ() + " IN PROGRESS: " +  pulsesRepository.countMarkBpulseKeyInProgress());
	        for(Object keyToProcess : keys) {
	        //for(Entry next : iterator.iterable()) {
	        	Long keyPulse = (Long) keyToProcess;
	        	//String keyPulse = new String(next.getKey());
	        	//obtain the associated pulse
	        	initGets = Calendar.getInstance().getTimeInMillis();
	        	PulsesRQ selectedPulsesRQ = null;
	        	try {
	        		selectedPulsesRQ = pulsesRepository.getBpulseRQByKey(keyPulse);
	        	} catch (Exception e) {
	    			logger.error("ERROR TIMER PROCESSING", e);
	    			continue;
	    		}
	        	//byte[] serializedPulsesRQ = pulsesRepository.getSerializedBpulseRQByKey(keyPulse);
	        	//PulsesRQ selectedPulsesRQ = null;
	        	/*try {
					if (serializedPulsesRQ != null) {
						selectedPulsesRQ = PulsesRQ.parseFrom(serializedPulsesRQ);
					}
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				}*/
	        	/*String keyMarkedAsInProgress = pulsesRepository.getMarkedBPulseKeyInProgress(keyPulse);
	        	//System.out.println("KEY TO PROCESS: " + keyPulse);
	        	if (keyMarkedAsInProgress != null) {
	        		continue;
	        	}*/
	        	summarizeGets = summarizeGets + (Calendar.getInstance().getTimeInMillis() - initGets);
	        	int totalPulsesOfCurrentKey = COMMON_NUMBER_0;
	        	if (selectedPulsesRQ != null) {
	        		totalPulsesOfCurrentKey = selectedPulsesRQ.getPulseList().size();
	        	}
	        	if (selectedPulsesRQ != null) {
	        		
	        		//mark bpulse key as INPROGRESS
	        		pulsesRepository.markBpulseKeyInProgress(keyPulse);
	        		totalOfProcessedPulses++;
	        		//System.out.println("CURRENT NUMBER OF PULSES TO PROCESS: " + cantidadpulsosreal + " " + Calendar.getInstance().getTime() + " GET AVERAGEMILLIS " + summarizeGets + " PULSE PROCESSING AVERAGE TIME: "  + summarizedTime);
	        		if (totalOfPulsesToSend + totalPulsesOfCurrentKey <= DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES) {
		        		
		        		init = Calendar.getInstance().getTimeInMillis();
		        		pulses.setVersion(selectedPulsesRQ.getVersion());
		        		pulses.addAllPulse(selectedPulsesRQ.getPulseList());
		        		
		        		totalOfPulsesToSend = totalOfPulsesToSend + totalPulsesOfCurrentKey;
		        		keyPulseListToDelete.add(keyPulse);
		        		summarizedTime = summarizedTime + (Calendar.getInstance().getTimeInMillis() - init);
		        		
		        	} else {
		        		//prepare to send the pulsesRQ to the RestService
		        		summarizedPulsesRQToSend = pulses.build();
		        		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete, 0);
		        		//restart the summarizedPulsesRQToSend for a new massiveRQ send
		        		summarizedPulsesRQToSend = null;
		        		pulses = PulsesRQ.newBuilder();
		        		pulses.setVersion(selectedPulsesRQ.getVersion());
		        		keyPulseListToDelete = new ArrayList<Long>();
		        		pulses.addAllPulse(selectedPulsesRQ.getPulseList());
		        		totalOfPulsesToSend = totalPulsesOfCurrentKey;
		        		keyPulseListToDelete.add(keyPulse);
		        	}
	        	}
	        	
	        	
	        }
	        
	        if (pulses != null && pulses.getPulseCount() > 0) {
	        	summarizedPulsesRQToSend = pulses.build();
	    		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete, 0);
	        }
	        logger.info("END TIMER PULSES PROCESSING...PROCESSED PULSES: " + totalOfProcessedPulses);
		} catch (Exception e) {
			logger.error("ERROR TIMER PROCESSING", e);
		}
	}
	
	/**
	 * Method that performs the pulses processing. It reads all the pending pulses from PULSESDB, for each of them, it marks as INPROGRESS and extracts the pulses list and add it to the summarizedPulsesRQToSend object.
	 * The summarizedPulsesRQToSend object will be sent to BPULSE REST SERVICE when it's pulses amount become equals to DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES.
	 * 
	 */
	private void executePartitioningRestPulsesSending() {
		
		try {
			
			IPulsesRepository pulsesRepository = bpulseSender.getPulsesRepository();
			logger.info("BEGIN TIMER PULSES PROCESSING TABLES IN PROGRESS: " +  pulsesRepository.countMarkBpulseTableInProgress());
			//obtain the current minute.
			Calendar currentTime = Calendar.getInstance();
			int currentMinute = currentTime.get(Calendar.MINUTE);
			//obtain the tableIndexList to get the pulses to send to BPulseRestService
			List<Integer> tableIndexList = getTableIndexListFromCurrentMinute(currentMinute, COMMON_NUMBER_MINUS_5, COMMON_NUMBER_5);  
			
			for(Integer tableIndex : tableIndexList) {
				
				if(!pulsesRepository.isAvailableBpulseTable(tableIndex)) {
					logger.info("TABLE_INDEX" + tableIndex + " IS IN PROGRESS YET");
					continue;
				}
				
				logger.info("BEGIN PULSES PROCESSING FOR TABLE_INDEX " + tableIndex + " RECORDS READ FROM DB: " + pulsesRepository.countBpulsesRQ(tableIndex));
				
				//obtain the current key list
		        Object[] keys = pulsesRepository.getSortedbpulseRQMapKeys(tableIndex);
		        List<Long> keyPulseListToDelete = new ArrayList<Long>();
		        
		        if(keys != null && keys.length > COMMON_NUMBER_0) {
		        	//blocks the current table to read pulses
					pulsesRepository.markBpulseTableInProgress(tableIndex);
		        }
		        
		        PulsesRQ summarizedPulsesRQToSend = null;
		        PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
		        int totalOfPulsesToSend = COMMON_NUMBER_0;
		        int totalOfProcessedPulses = COMMON_NUMBER_0;
		        long summarizedTime = COMMON_NUMBER_0;
		        long init = COMMON_NUMBER_0;
		        long initGets = COMMON_NUMBER_0;
		        long summarizeGets = COMMON_NUMBER_0;
		        
		        for(Object keyToProcess : keys) {
		        //for(Entry next : iterator.iterable()) {
		        	Long keyPulse = (Long) keyToProcess;
		        	//String keyPulse = new String(next.getKey());
		        	//obtain the associated pulse
		        	initGets = Calendar.getInstance().getTimeInMillis();
		        	PulsesRQ selectedPulsesRQ = null;
		        	try {
		        		selectedPulsesRQ = pulsesRepository.getBpulseRQByKey(keyPulse, tableIndex);
		        	} catch (Exception e) {
		    			logger.error("ERROR TIMER PROCESSING", e);
		    			continue;
		    		}
		        	
		        	summarizeGets = summarizeGets + (Calendar.getInstance().getTimeInMillis() - initGets);
		        	int totalPulsesOfCurrentKey = COMMON_NUMBER_0;
		        	if (selectedPulsesRQ != null) {
		        		totalPulsesOfCurrentKey = selectedPulsesRQ.getPulseList().size();
		        	}
		        	if (selectedPulsesRQ != null) {
		        		
		        		//mark bpulse key as INPROGRESS
		        		//pulsesRepository.markBpulseTableInProgress(tableIndex);
		        		totalOfProcessedPulses++;
		        		//System.out.println("CURRENT NUMBER OF PULSES TO PROCESS: " + cantidadpulsosreal + " " + Calendar.getInstance().getTime() + " GET AVERAGEMILLIS " + summarizeGets + " PULSE PROCESSING AVERAGE TIME: "  + summarizedTime);
		        		//if (totalOfPulsesToSend + totalPulsesOfCurrentKey <= DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES) {
			        		
			        	init = Calendar.getInstance().getTimeInMillis();
			        	pulses.setVersion(selectedPulsesRQ.getVersion());
			        	pulses.addAllPulse(selectedPulsesRQ.getPulseList());
			        		
			        	totalOfPulsesToSend = totalOfPulsesToSend + totalPulsesOfCurrentKey;
			        	keyPulseListToDelete.add(keyPulse);
			        	summarizedTime = summarizedTime + (Calendar.getInstance().getTimeInMillis() - init);
			        		
			        	/*} else {
			        		//prepare to send the pulsesRQ to the RestService
			        		summarizedPulsesRQToSend = pulses.build();
			        		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete);
			        		//restart the summarizedPulsesRQToSend for a new massiveRQ send
			        		summarizedPulsesRQToSend = null;
			        		pulses = PulsesRQ.newBuilder();
			        		pulses.setVersion(selectedPulsesRQ.getVersion());
			        		keyPulseListToDelete = new ArrayList<Long>();
			        		pulses.addAllPulse(selectedPulsesRQ.getPulseList());
			        		totalOfPulsesToSend = totalPulsesOfCurrentKey;
			        		keyPulseListToDelete.add(keyPulse);
			        	}*/
		        	}
		        	
		        	
		        }
		        
		        if (pulses != null && pulses.getPulseCount() > 0) {
		        	summarizedPulsesRQToSend = pulses.build();
		    		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete, tableIndex);
		        }
		        
		        logger.info("END PULSES PROCESSING FOR TABLE_INDEX " + tableIndex + "...PROCESSED PULSES: " + totalOfProcessedPulses);
	        
			}
	        
	        logger.info("END TIMER PULSES PROCESSING...");
		} catch (Exception e) {
			logger.error("ERROR TIMER PROCESSING", e);
		}
	}

	private void invokeBPulseRestService(BPulseSender client, PulsesRQ summarizedPulsesRQToSend,
			List<Long> keysToDelete, int tableIndex) {
		client.executeBPulseRestService(summarizedPulsesRQToSend, keysToDelete, tableIndex, this.dbMode);
	}
	
	private List<Integer> getTableIndexListFromCurrentMinute(int currentMinute, int minOffset, int maxOffset) {
		
		List<Integer> resp = new ArrayList<Integer>();
		
		int minIndex = (COMMON_NUMBER_60 + currentMinute + minOffset) % COMMON_NUMBER_60;
		int maxIndex = (COMMON_NUMBER_60 + currentMinute + maxOffset) % COMMON_NUMBER_60;
		logger.info("InitialTableIndex: " + minIndex + " LastTableIndex: " + maxIndex);
		resp.add(minIndex);
		int tempIndex = minIndex;
		while (tempIndex != maxIndex) {
			
			tempIndex = (COMMON_NUMBER_60 + tempIndex - 1) % COMMON_NUMBER_60;
			resp.add(tempIndex);
			
		}
		
		return resp;
		
	}

}

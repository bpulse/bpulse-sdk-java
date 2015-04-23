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
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseRestSenderTimer extends TimerTask{
	
	private BPulseSender bpulseSender;
	//final static Logger logger = Logger.getLogger(BPulseRestSenderTimer.class);
	final static Logger logger = LoggerFactory.getLogger(BPulseRestSenderTimer.class);
	
	public BPulseRestSenderTimer(BPulseSender pBpulseSender) {
		this.bpulseSender = pBpulseSender;
	}
	
	@Override
	public void run() {
		Calendar c = Calendar.getInstance();
		executeRestPulsesSending();
		
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
	        	PulsesRQ selectedPulsesRQ = pulsesRepository.getBpulseRQByKey(keyPulse);
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
		        		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete);
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
	    		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete);
	        }
	        logger.info("END TIMER PULSES PROCESSING...PROCESSED PULSES: " + totalOfProcessedPulses);
		} catch (Exception e) {
			logger.error("ERROR TIMER PROCESSING", e);
		}
	}

	private void invokeBPulseRestService(BPulseSender client, PulsesRQ summarizedPulsesRQToSend,
			List<Long> keysToDelete) {
		client.executeBPulseRestService(summarizedPulsesRQToSend, keysToDelete);
	}

}

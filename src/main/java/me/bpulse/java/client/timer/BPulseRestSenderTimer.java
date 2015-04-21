package me.bpulse.java.client.timer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimerTask;

import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.pulsesclient.BPulseJavaClient;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.pulsesrepository.PulsesRepository;

import com.google.protobuf.InvalidProtocolBufferException;

import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES;

public class BPulseRestSenderTimer extends TimerTask{
	
	private BPulseSender bpulseSender;
	private int maxNumberPulsesToReadFromDB;
	
	public BPulseRestSenderTimer(BPulseSender pBpulseSender, int maxNumberRQsFromDB) {
		this.bpulseSender = pBpulseSender;
		this.maxNumberPulsesToReadFromDB = maxNumberRQsFromDB;
	}
	
	@Override
	public void run() {
		Calendar c = Calendar.getInstance();
		System.out.println("EJECUCION DE TIMER BPULSE..." + c.getTime());
		executeRestPulsesSending();
		
	}
	
	private void executeRestPulsesSending() {
		
		//obtain the current key list
		IPulsesRepository pulsesRepository = bpulseSender.getPulsesRepository();
        Object[] keys = pulsesRepository.getSortedbpulseRQMapKeys();
		//EntryIterator iterator = pulsesRepository.getIterableEntriesBpulseRQMapKeys();
        List<String> keyPulseListToDelete = new ArrayList<String>();
        
        PulsesRQ summarizedPulsesRQToSend = null;
        PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
        int totalKeysRead = COMMON_NUMBER_0;
        int totalOfPulsesToSend = COMMON_NUMBER_0;
        int cantidadpulsosreal = COMMON_NUMBER_0;
        long summarizedTime = COMMON_NUMBER_0;
        long init = COMMON_NUMBER_0;
        long initGets = COMMON_NUMBER_0;
        long summarizeGets = COMMON_NUMBER_0;
        System.out.println("INICIO PROCESAMIENTO PULSOS..." + " RECORDS READ FROM DB: " + pulsesRepository.countBpulsesRQ() + " INSERTED RECORDS: " + pulsesRepository.getInsertedRecords() + " EN PROCESO " +  pulsesRepository.countMarkBpulseKeyInProgress() + " " + Calendar.getInstance().getTime());
        for(Object keyToProcess : keys) {
        //for(Entry next : iterator.iterable()) {
        	totalKeysRead++;
        	if (totalKeysRead > maxNumberPulsesToReadFromDB) {
        		break;
        	}
        	String keyPulse = (String) keyToProcess;
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
        		cantidadpulsosreal++;
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
	        		System.out.println("PREPARE TO SEND SUMMARIZED PULSESRQ");
	        		invokeBPulseRestService(bpulseSender, summarizedPulsesRQToSend, keyPulseListToDelete);
	        		//restart the summarizedPulsesRQToSend for a new massiveRQ send
	        		summarizedPulsesRQToSend = null;
	        		pulses = PulsesRQ.newBuilder();
	        		pulses.setVersion(selectedPulsesRQ.getVersion());
	        		keyPulseListToDelete = new ArrayList<String>();
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
        System.out.println("FIN PROCESAMIENTO PULSOS...CANTIDAD " + cantidadpulsosreal + " " + Calendar.getInstance().getTime());
		
	}

	private void invokeBPulseRestService(BPulseSender client, PulsesRQ summarizedPulsesRQToSend,
			List<String> keysToDelete) {
		client.executeBPulseRestService(summarizedPulsesRQToSend, keysToDelete);
	}

}

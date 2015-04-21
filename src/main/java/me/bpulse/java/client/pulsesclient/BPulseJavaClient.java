package me.bpulse.java.client.pulsesclient;

import java.util.Timer;
import java.util.TimerTask;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.timer.BPulseRestSenderTimer;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_TIMER_DELAY;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_1000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_180000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MIN_DELAY;


public class BPulseJavaClient {
	
	private static boolean isStarted = false;
	private static BPulseJavaClient instance;
	private BPulseSender bpulseSender;
	
	protected BPulseJavaClient() {
		bpulseSender = BPulseSender.getInstance();
		//isStarted = true;
		start();
	}
	
	public synchronized static BPulseJavaClient getInstance() {
		
		if(instance == null) {
			instance = new BPulseJavaClient();
		}
		
		return instance;
	}
	
	private synchronized void start() {
		
		if (!isStarted) {
			
			String propMaxNumberRQsToReadFromDB = PropertiesManager.getProperty(BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER);
			int maxNumberRQsToReadFromDB = COMMON_NUMBER_0;
			
			if (propMaxNumberRQsToReadFromDB != null) {
				maxNumberRQsToReadFromDB = Integer.parseInt(propMaxNumberRQsToReadFromDB);
			} else {
				maxNumberRQsToReadFromDB = COMMON_NUMBER_180000;
			}
			
			TimerTask timerTask = new BPulseRestSenderTimer(bpulseSender, maxNumberRQsToReadFromDB);
	        Timer timer = new Timer();
	        String periodInMinutesNextExecutionTimer = PropertiesManager.getProperty(BPULSE_PROPERTY_USER_TIMER_DELAY);
	        long periodInMillis = COMMON_NUMBER_0;
	        if (periodInMinutesNextExecutionTimer == null) {
	        	periodInMillis = DEFAULT_TIMER_MIN_DELAY;
	        } else {
	        	periodInMillis = Long.parseLong(periodInMinutesNextExecutionTimer)*COMMON_NUMBER_60*COMMON_NUMBER_1000;
	        }
	        timer.schedule(timerTask, COMMON_NUMBER_0, periodInMillis);
	        isStarted = true;
		}
		
	}
	
	public String sendPulse(PulsesRQ pulse) throws Exception {
		if (isStarted) {
			return bpulseSender.sendPulse(pulse);
		} else {
			throw new Exception("Error sending pulses: BPulseJavaClient is not started yet. Please invoke the start() method.");
		}
	}
	
	public BPulseSender getBPulseSender() {
		return bpulseSender;
	}

}

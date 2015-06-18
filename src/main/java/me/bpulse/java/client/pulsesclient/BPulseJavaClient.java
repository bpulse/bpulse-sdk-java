/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.pulsesclient;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.timer.BPulseRestSenderTimer;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_PULSES_REPOSITORY_MODE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_TIMER_DELAY;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_1000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_180000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MIN_DELAY;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_MEM_PULSES_REPOSITORY;

import org.slf4j.*;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseJavaClient {
	
	private static boolean isStarted = false;
	private static BPulseJavaClient instance;
	private BPulseSender bpulseSender;
	final static Logger logger = LoggerFactory.getLogger("bpulseLogger");
	
	protected BPulseJavaClient() throws Exception {
		logger.info("GET INSTANCE BpulseJavaClient...");
		bpulseSender = BPulseSender.getInstance();
		//isStarted = true;
		start();
		logger.info("GET INSTANCE BpulseJavaClient SUCCESSFUL.");
	}
	
	/**
	 * Method that performs the get singleton instance from BPulseJavaClient
	 * 
	 */
	public synchronized static BPulseJavaClient getInstance() throws Exception {
		if(instance == null) {
			instance = new BPulseJavaClient();
		}
		return instance;
	}
	
	/**
	 * Method that performs the init BPulse Timer from BPulseJavaClient
	 * 
	 */
	private synchronized void start() {
		
		try {
			logger.info("INIT BPULSE TIMER...");
			if (!isStarted) {
				String propDBMode = PropertiesManager.getProperty(BPULSE_PROPERTY_PULSES_REPOSITORY_MODE);
				if(propDBMode == null) {
					propDBMode = BPULSE_MEM_PULSES_REPOSITORY;
				}
				TimerTask timerTask = new BPulseRestSenderTimer(bpulseSender, propDBMode);
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
		} catch (Exception e) {
			logger.error("FAILED TO INIT BPULSE TIMER: ", e);
		}
		
	}
	
	/**
	 * Method that performs the pulses sending to BPULSE
	 * 
	 * @param pulse The Pulse in Protobuf format.
	 * @return Status of the pulse sending.
	 */
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

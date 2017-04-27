/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.pulsesclient;

import static me.bpulse.java.client.common.BPulseConstants.BPULSE_MEM_PULSES_REPOSITORY;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_PULSES_REPOSITORY_MODE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_TIMER_DELAY;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_1000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_TIMER_MIN_DELAY;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.Value;
import me.bpulse.java.client.compress.CompressUtil;
import me.bpulse.java.client.dto.AttributeDto;
import me.bpulse.java.client.properties.PropertiesManager;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.timer.BPulseRestSenderTimer;

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
		logger.debug("GET INSTANCE BpulseJavaClient...");
		bpulseSender = BPulseSender.getInstance();
		start();
		logger.debug("GET INSTANCE BpulseJavaClient SUCCESSFUL.");
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
			logger.debug("INIT BPULSE TIMER...");
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
	 * @param pulse - The Pulse in Protobuf format.
	 * @return Status of the pulse sending.
	 * @throws Exception 
	 */
	public String sendPulse(PulsesRQ pulse) throws Exception {
		if (isStarted) {
			return bpulseSender.sendPulse(pulse);
		} else {
			throw new Exception("Error sending pulses: BPulseJavaClient is not started yet. Please invoke the start() method.");
		}
	}
	/**
	 * Method that performs the pulses sending to BPULSE
	 * @param pulse - The Pulse in Protobuf format. 
	 * @param listLong - Names of Pulse attributes that is Long.
	 * @return Status of the pulse sending.
	 * @throws Exception
	 */
	public String sendPulseWithLong(PulsesRQ pulse, List<AttributeDto> listLong) throws Exception {
		if (isStarted) {
			pulse = rebuildValue(pulse, listLong,false); 
			return bpulseSender.sendPulse(pulse);
		} else {
			throw new Exception("Error sending pulses: BPulseJavaClient is not started yet. Please invoke the start() method.");
		}
	}

	/**
	 * Method that performs the pulses sending to BPULSE
	 * @param pulse - The Pulse in Protobuf format.
	 * @param listTrace - Names of Pulse attributes that is Trace.
	 * @return Status of the pulse sending.
	 * @throws Exception
	 */
	public String sendPulseWithTrace(PulsesRQ pulse, List<AttributeDto> listTrace) throws Exception {
		if (isStarted) {
			pulse = rebuildValue(pulse, listTrace,true); 
			return bpulseSender.sendPulse(pulse);
		} else {
			throw new Exception("Error sending pulses: BPulseJavaClient is not started yet. Please invoke the start() method.");
		}
	}
	/**
	 * Method that performs the pulses sending to BPULSE
	 * @param pulse - The Pulse in Protobuf format.
	 * @param listLong - Names of Pulse attributes that is Long.
	 * @param listTrace - Names of Pulse attributes that is Trace.
	 * @return Status of the pulse sending.
	 * @throws Exception
	 */
	public String sendPulse(PulsesRQ pulse, List<AttributeDto> listLong, List<AttributeDto> listTrace) throws Exception {
		if (isStarted) {
			if(isRepeatAttrList(listLong,listTrace)){
				throw new Exception("Error sending pulses: Exist some similar attributes in the Long list and the Trace list.");
			}
			pulse = rebuildValue(pulse, listLong,false); 
			pulse = rebuildValue(pulse, listTrace,true);
			return bpulseSender.sendPulse(pulse);
		} else {
			throw new Exception("Error sending pulses: BPulseJavaClient is not started yet. Please invoke the start() method.");
		}
	}
	
	/**
	 * Valid if exist some similar attributes in the Long list and the Trace list
	 * @param listLong - Names of Pulse attributes that is Long.
	 * @param listTrace - Names of Pulse attributes that is Trace.
	 * @return true - Exist <br>
	 * 			false - Not Exist
	 */
	private boolean isRepeatAttrList(List<AttributeDto> listLong, List<AttributeDto> listTrace) {
		if(listLong!=null && listLong.size() > 0
			&& listTrace!=null && listTrace.size() > 0){
			for (AttributeDto longValue : listLong) {
				AttributeDto traceValue = listTrace.get(listTrace.indexOf(longValue));
				if(traceValue!=null){
					for (String attrLong : longValue.getListAttr()) {
						if(traceValue.getListAttr().contains(attrLong)){
							logger.error("Error : The '"+attrLong+"' attribute of TypeId '"+longValue.getTypeId()+"' is repeated in the Trace list.");
							return true;
						}
					}
				}
				
			}
			
		}
		return false;
	}
	/**
	 * Rebuild values if it is TraceData or LongData
	 * @param pulse - The Pulse in Protobuf format.
	 * @param listAttr - The list of attributes.
	 * @param isTrace - is Trace.
	 */
	private PulsesRQ rebuildValue(PulsesRQ pulse, List<AttributeDto> listAttr,boolean isTrace) {
		if(listAttr==null || listAttr.isEmpty()){
			return pulse;
		}
		//transform list Atributes to map atributes
		Map<String, List<String>> mapAttr= listAttrtoMap(listAttr);
		PulsesRQ.Builder rqbuilder = pulse.toBuilder();
		for (Pulse.Builder pulseValue : rqbuilder.getPulseBuilderList()) {
			//get the list of attributes from the map
			List<String> listAttributes= mapAttr.get(pulseValue.getTypeId());
			if(listAttributes!=null && listAttributes.size()>0 ){
				for (Value.Builder value : pulseValue.getValuesBuilderList()) {
					if(listAttributes.contains(value.getName()) && value.getValuesCount()>0){
						//Build the new value
						 if(isTrace){
							 try {
								 value.setValues(0,CompressUtil.compress(value.getValues(0)));
							} catch (IOException e) {
								continue;
							}
						 }else{
							 value.setValues(0,Base64.encodeBase64String(value.getValues(0).getBytes()));
						 }
					}
				}
			}
		}
		return rqbuilder.build();
	}
	/**
	 * Transform the list of attributes to map 
	 * @param listLong - List of attributes.
	 * @return Map with the attributes.
	 */
	private Map<String, List<String>> listAttrtoMap(List<AttributeDto> listLong) {
		Map<String, List<String>> map = new HashMap<String, List<String>>();
		if(listLong!=null && listLong.size()>0){
			for (AttributeDto attributeDto : listLong) {
				map.put(attributeDto.getTypeId(), attributeDto.getListAttr());
			}
		}
		return map;
	}
	public BPulseSender getBPulseSender() {
		return bpulseSender;
	}

}

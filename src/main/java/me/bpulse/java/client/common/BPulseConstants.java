/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.common;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseConstants {
	
	public static final long DEFAULT_TIMER_MIN_DELAY = 60000;
	public static final long DEFAULT_TIMER_MAX_NUMBER_GROUPED_PULSES = 1000;
	public static final int DEFAULT_REST_INVOKER_TIMEOUT = 30000;
	public static final long DEFAULT_MAX_PULSESDB_SIZE_BYTES = 1073741824;
	public static final int COMMON_NUMBER_60 = 60;
	public static final int COMMON_NUMBER_1000 = 1000;
	public static final int COMMON_NUMBER_0 = 0;
	public static final int COMMON_NUMBER_1 = 1;
	public static final int COMMON_NUMBER_2 = 2;
	public static final int COMMON_NUMBER_3 = 3;
	public static final int COMMON_NUMBER_5 = 5;
	public static final int COMMON_NUMBER_MINUS_5 = -5;
	public static final int COMMON_NUMBER_180000 = 180000;
	public static final int BPULSE_REST_HTTP_CREATED = 201;
	
	public static final String BPULSE_SUCCESSFUL_RESPONSE = "OK";
	public static final String BPULSE_FAILED_RESPONSE = "ERROR";
	public static final String BPULSE_STATUS_PENDING = "P";
	public static final String BPULSE_STATUS_INPROGRESS = "I";
	public static final String BPULSE_REPOSITORY_NAME = "BPULSEDB";
	public static final String BPULSE_REPOSITORY_USER = "admin";
	public static final String BPULSE_PROPERTY_CONFIG_FILE = "bpulse.client.config";
	public static final String BPULSE_PROPERTY_USER_TIMER_DELAY = "bpulse.client.periodInMinutesNextExecTimer";
	public static final String BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES = "bpulse.client.initNumThreadsSendPulses";
	public static final String BPULSE_PROPERTY_NUMBER_THREADS_REST_INVOKER = "bpulse.client.initNumThreadsRestInvoker";
	public static final String BPULSE_PROPERTY_USER_CREDENTIALS_USERNAME = "bpulse.client.bpulseUsername";
	public static final String BPULSE_PROPERTY_USER_CREDENTIALS_PASSWORD = "bpulse.client.bpulsePassword";
	public static final String BPULSE_PROPERTY_URL_REST_SERVICE = "bpulse.client.bpulseRestURL";
	public static final String BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER = "bpulse.client.maxNumberPulsesReadFromTimer";
	public static final String BPULSE_PROPERTY_MAX_PULSESDB_SIZE_BYTES = "bpulse.client.pulsesRepositoryDBMaxSizeBytes";
	public static final String BPULSE_PROPERTY_PULSES_REPOSITORY_MODE = "bpulse.client.pulsesRepositoryMode";
	public static final String BPULSE_PROPERTY_MEM_PULSES_REPOSITORY_MAX_NUM_PULSES = "bpulse.client.pulsesRepositoryMemMaxNumberPulses";
	public static final String BPULSE_MEM_PULSES_REPOSITORY = "MEM";
	public static final String BPULSE_DB_PULSES_REPOSITORY = "DB";

}

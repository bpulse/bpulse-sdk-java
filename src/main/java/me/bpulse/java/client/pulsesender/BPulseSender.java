/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.pulsesender;

import java.util.List;
import java.util.Random;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;
import me.bpulse.java.client.pulsesrepository.H2PulsesRepository;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.pulsesrepository.InMemoryPulsesRepository;
import me.bpulse.java.client.rest.RestInvoker;
import me.bpulse.java.client.thread.BPulseRestSenderThread;
import me.bpulse.java.client.thread.PulsesSenderThread;
import me.bpulse.java.client.thread.ThreadPoolManager;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MAX_PULSESDB_SIZE_BYTES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_REST_INVOKER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_URL_REST_SERVICE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_CREDENTIALS_PASSWORD;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_CREDENTIALS_USERNAME;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_SUCCESSFUL_RESPONSE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_FAILED_RESPONSE;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_180000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_5;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_REST_INVOKER_TIMEOUT;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_MAX_PULSESDB_SIZE_BYTES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_PULSES_REPOSITORY_MODE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MEM_PULSES_REPOSITORY_MAX_NUM_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_MEM_PULSES_REPOSITORY;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_DB_PULSES_REPOSITORY;

import org.slf4j.*;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseSender {
	
	private static BPulseSender instance = null;
	private ThreadPoolManager persistPulsesThreadPool;
	private ThreadPoolManager sendPulsesByRestThreadPool;
	private IPulsesRepository pulsesRepository;
	private RestInvoker restInvoker;
	private String bpulseRestUrl;
	private long maxDBSizeBytes;
	private long maxMemNumPulses;
	private String propDBMode;
	final static Logger logger = LoggerFactory.getLogger("bpulseLogger");
	
	
	/**
	 * Constructor that performs the init PulsesRepository for the pulses storage.
	 * 
	 */
	protected BPulseSender() throws Exception {
		
		String propMaxNumberRQsToReadFromDB = PropertiesManager.getProperty(BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER);
		int maxNumberRQsToReadFromDB = COMMON_NUMBER_0;
		
		if (propMaxNumberRQsToReadFromDB != null) {
			maxNumberRQsToReadFromDB = Integer.parseInt(propMaxNumberRQsToReadFromDB);
		} else {
			maxNumberRQsToReadFromDB = COMMON_NUMBER_180000;
		}
		
		String propMaxDBSize = PropertiesManager.getProperty(BPULSE_PROPERTY_MAX_PULSESDB_SIZE_BYTES);
		if (propMaxDBSize != null) {
			maxDBSizeBytes = Long.parseLong(propMaxDBSize);
		} else {
			maxDBSizeBytes = DEFAULT_MAX_PULSESDB_SIZE_BYTES;
		}
		
		propDBMode = PropertiesManager.getProperty(BPULSE_PROPERTY_PULSES_REPOSITORY_MODE);
		String propMemMaxNumPulses = PropertiesManager.getProperty(BPULSE_PROPERTY_MEM_PULSES_REPOSITORY_MAX_NUM_PULSES);
		
		if(propDBMode == null) {
			propDBMode = BPULSE_MEM_PULSES_REPOSITORY;
		}
		if (propMemMaxNumPulses != null) {
			maxMemNumPulses = Long.parseLong(propMemMaxNumPulses);
		} else {
			maxMemNumPulses = 1000000L;//TODO CONSTANTE
		}
		
		if(propDBMode.equals(BPULSE_DB_PULSES_REPOSITORY)) {
			logger.info("H2PulsesRepository instance");
			pulsesRepository = new H2PulsesRepository(maxNumberRQsToReadFromDB);
		} else if(propDBMode.equals(BPULSE_MEM_PULSES_REPOSITORY)) {
			logger.info("InMemoryPulsesRepository instance");
			pulsesRepository = new InMemoryPulsesRepository(maxNumberRQsToReadFromDB);
		}
		initThreadPoolManager();
		initRestInvoker();
	}
	
	/**
	 * Method that performs the get singleton instance from BPulseSender
	 * 
	 */
	public static BPulseSender getInstance() throws Exception {
		if (instance == null) {
			instance = new BPulseSender();
		}
		
		return instance;
	}
	
	/**
	 * method that performs the initialization of thread pools for the pulses storage and pulses notification through BPULSE REST SERVICE INVOCATION.
	 * 
	 */
	private void initThreadPoolManager() {
		int pPoolSizeSendPulses;
		int pPoolSizeRestInvoker;
		int pMaxPoolSize;
		long pKeepAliveTimeMilliseconds;
		//obtain thread initialization properties
		String initialPoolSizeSendPulses = "" + COMMON_NUMBER_5;//PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES);
		String initialPoolSizeRestInvoker = "" + COMMON_NUMBER_60;//PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_REST_INVOKER);
		
		
		if (initialPoolSizeSendPulses != null) {
			pPoolSizeSendPulses = Integer.parseInt(initialPoolSizeSendPulses);
		} else {
			pPoolSizeSendPulses = COMMON_NUMBER_5;
		}
		if (initialPoolSizeRestInvoker != null) {
			pPoolSizeRestInvoker = Integer.parseInt(initialPoolSizeRestInvoker);
		} else {
			pPoolSizeRestInvoker = COMMON_NUMBER_5;
		}
		
		persistPulsesThreadPool = new ThreadPoolManager(pPoolSizeSendPulses);
		sendPulsesByRestThreadPool = new ThreadPoolManager(pPoolSizeRestInvoker);
		
	}
	
	/**
	 * method that performs the initialization of BPULSE REST SERVICE INVOCATION.
	 * 
	 */
	private void initRestInvoker() {
		
		String username = PropertiesManager.getProperty(BPULSE_PROPERTY_USER_CREDENTIALS_USERNAME);
		String password = PropertiesManager.getProperty(BPULSE_PROPERTY_USER_CREDENTIALS_PASSWORD);
		String url = PropertiesManager.getProperty(BPULSE_PROPERTY_URL_REST_SERVICE);
		int defaultRestTimeOut = DEFAULT_REST_INVOKER_TIMEOUT;
				
		restInvoker = new RestInvoker (username, password, defaultRestTimeOut);
		bpulseRestUrl = url;
	}
	
	/**
	 * Method that performs the pulses sending to BPULSE. Verify the maximum DB file size defined vs the current size of the BPULSEDB to make the insertion of the pulse.
	 * 
	 * @param pulse The Pulse in Protobuf format.
	 * @return Status of the pulse sending.
	 */
	public synchronized String sendPulse(PulsesRQ pulse) {
		Random random = new Random();
		long additionalPulseId = random.nextLong();
		long maxRepositorySize = 0;
		
		if(propDBMode.equals(BPULSE_DB_PULSES_REPOSITORY)) {
			maxRepositorySize = maxDBSizeBytes;
		} else if(propDBMode.equals(BPULSE_MEM_PULSES_REPOSITORY)) {
			maxRepositorySize = maxMemNumPulses;
		}
		
		if (maxRepositorySize > this.pulsesRepository.getDBSize()) {
			PulsesSenderThread thread = new PulsesSenderThread("THREAD-"+System.currentTimeMillis() + additionalPulseId, pulse, this.pulsesRepository);
			persistPulsesThreadPool.getThreadPoolExecutor().execute(thread);
			return BPULSE_SUCCESSFUL_RESPONSE;
		} else {
			logger.error("FAILED SEND PULSE: THE MAX PULSESDB SIZE WAS REACHED, MAXIMUM: " + maxDBSizeBytes + ", CURRENT SIZE: " + this.pulsesRepository.getDBSize());
			return BPULSE_FAILED_RESPONSE;
		}
	}
	
	/**
	 * Method that performs the pulses sending to BPULSE REST SERVICE. If REST notification is OK, it performs the deletion of the sent pulses from the PULSESDB.
	 * 
	 * @param summarizedPulsesRQToSend The PulsesRQ in Protobuf format to send to the BPULSE REST SERVICE.
	 * @param keysToDelete The Pulse Key List of the pulses to delete from the PULSESDB.
	 */
	public synchronized void executeBPulseRestService(PulsesRQ summarizedPulsesRQToSend,
			List<Long> keysToDelete, int tableIndex, String dbMode) {
		Random random = new Random();
		long additionalPulseId = random.nextLong();
		sendPulsesByRestThreadPool.getThreadPoolExecutor().execute(new BPulseRestSenderThread("THREAD-"+System.currentTimeMillis() + additionalPulseId, summarizedPulsesRQToSend, this.pulsesRepository, keysToDelete, restInvoker, bpulseRestUrl, tableIndex, dbMode));
	}
	
	public IPulsesRepository getPulsesRepository() {
		return pulsesRepository;
	}
	
}

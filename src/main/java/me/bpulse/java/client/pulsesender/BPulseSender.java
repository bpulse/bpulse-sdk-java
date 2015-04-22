package me.bpulse.java.client.pulsesender;

import java.util.List;
import java.util.Random;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;
import me.bpulse.java.client.pulsesrepository.EhCachePulsesRepository;
import me.bpulse.java.client.pulsesrepository.H2PulsesRepository;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.pulsesrepository.LMDBRepository;
import me.bpulse.java.client.pulsesrepository.PulsesRepository;
import me.bpulse.java.client.rest.RestInvoker;
import me.bpulse.java.client.thread.BPulseRestSenderThread;
import me.bpulse.java.client.thread.PulsesSenderThread;
import me.bpulse.java.client.thread.ThreadPoolManager;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_REST_INVOKER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_URL_REST_SERVICE;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_CREDENTIALS_PASSWORD;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_USER_CREDENTIALS_USERNAME;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_SUCCESSFUL_RESPONSE;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_180000;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_5;
import static me.bpulse.java.client.common.BPulseConstants.DEFAULT_REST_INVOKER_TIMEOUT;

public class BPulseSender {
	
	private static BPulseSender instance = null;
	private ThreadPoolManager persistPulsesThreadPool;
	private ThreadPoolManager sendPulsesByRestThreadPool;
	private IPulsesRepository pulsesRepository;
	private RestInvoker restInvoker;
	private String bpulseRestUrl;
	
	
	protected BPulseSender() {
		//pulsesRepository = new PulsesRepository();
		//pulsesRepository = new EhCachePulsesRepository();
		//pulsesRepository = new LMDBRepository();
		
		String propMaxNumberRQsToReadFromDB = PropertiesManager.getProperty(BPULSE_PROPERTY_MAX_NUMBER_PULSES_TO_PROCESS_TIMER);
		int maxNumberRQsToReadFromDB = COMMON_NUMBER_0;
		
		if (propMaxNumberRQsToReadFromDB != null) {
			maxNumberRQsToReadFromDB = Integer.parseInt(propMaxNumberRQsToReadFromDB);
		} else {
			maxNumberRQsToReadFromDB = COMMON_NUMBER_180000;
		}
		
		pulsesRepository = new H2PulsesRepository(maxNumberRQsToReadFromDB);
		initThreadPoolManager();
		initRestInvoker();
	}
	
	public static BPulseSender getInstance() {
		if (instance == null) {
			instance = new BPulseSender();
		}
		
		return instance;
	}
	
	private void initThreadPoolManager() {
		int pPoolSizeSendPulses;
		int pPoolSizeRestInvoker;
		int pMaxPoolSize;
		long pKeepAliveTimeMilliseconds;
		//obtain thread initialization properties
		String initialPoolSizeSendPulses = PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES);
		String initialPoolSizeRestInvoker = PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_REST_INVOKER);
		
		
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
	
	private void initRestInvoker() {
		
		String username = PropertiesManager.getProperty(BPULSE_PROPERTY_USER_CREDENTIALS_USERNAME);
		String password = PropertiesManager.getProperty(BPULSE_PROPERTY_USER_CREDENTIALS_PASSWORD);
		String url = PropertiesManager.getProperty(BPULSE_PROPERTY_URL_REST_SERVICE);
		int defaultRestTimeOut = DEFAULT_REST_INVOKER_TIMEOUT;
				
		restInvoker = new RestInvoker (username, password, defaultRestTimeOut);
		bpulseRestUrl = url;
	}
	
	public synchronized String sendPulse(PulsesRQ pulse) {
		Random random = new Random();
		long additionalPulseId = random.nextLong();
		persistPulsesThreadPool.getThreadPoolExecutor().execute(new PulsesSenderThread("THREAD-"+System.currentTimeMillis() + additionalPulseId, pulse, this.pulsesRepository));
		return BPULSE_SUCCESSFUL_RESPONSE;
	}
	
	public synchronized void executeBPulseRestService(PulsesRQ summarizedPulsesRQToSend,
			List<Long> keysToDelete) {
		Random random = new Random();
		long additionalPulseId = random.nextLong();
		sendPulsesByRestThreadPool.getThreadPoolExecutor().execute(new BPulseRestSenderThread("THREAD-"+System.currentTimeMillis() + additionalPulseId, summarizedPulsesRQToSend, this.pulsesRepository, keysToDelete, restInvoker, bpulseRestUrl));
	}
	
	public IPulsesRepository getPulsesRepository() {
		return pulsesRepository;
	}
	
}

/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.thread;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
//import org.apache.log4j.Logger;
import org.slf4j.*;

import com.google.protobuf.Message;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.domain.proto.collector.CollectorMessageRS.PulsesRS;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.rest.ExtraResponse;
import me.bpulse.java.client.rest.RestInvoker;
import me.bpulse.java.client.rest.RestInvoker.TestContentType;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REST_HTTP_CREATED;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class BPulseRestSenderThread implements Runnable{

	private PulsesRQ pulseToSendByRest;
	private IPulsesRepository dbPulsesRepository;
	private List<Long> keysToDelete;
	private RestInvoker restInvoker;
	private String bpulseRestURL;
	private String id;
	final static Logger logger = LoggerFactory.getLogger(BPulseRestSenderThread.class);
	
	public BPulseRestSenderThread(String pThreadId, PulsesRQ pPulseToSendByRest, IPulsesRepository pDbPulsesRepository,List<Long> pKeysToDelete, RestInvoker pRestInvoker, String pUrl) {
		this.pulseToSendByRest = pPulseToSendByRest;
		this.dbPulsesRepository = pDbPulsesRepository;
		this.keysToDelete = pKeysToDelete;
		this.id = pThreadId;
		this.restInvoker = pRestInvoker;
		this.bpulseRestURL = pUrl;
	}
	
	@Override
	public void run() {
		try {
			invokeRestService();
			deletePulseKeysProcessedByRest();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
			logger.error("FAILED INVOKING BPULSE REST SERVICE: " + e.getMessage());
			try {
				releaseCurrentBPulseInProgressKeys();
			} catch (Exception e1) {
			}
		} catch (Exception e) {
			logger.error("FAILED INVOKING BPULSE REST SERVICE: " + e.getMessage());
			try {
				releaseCurrentBPulseInProgressKeys();
			} catch (Exception e1) {
			}
		}
	}
	
	/**
	 * Method that performs the call of the releaseBpulseKeyInProgressByKey method.
	 * 
	 */
	private synchronized void releaseCurrentBPulseInProgressKeys() throws Exception {
		for(Long keyToDelete : this.keysToDelete) {
			this.dbPulsesRepository.releaseBpulseKeyInProgressByKey(keyToDelete);
		}
	}

	/**
	 * Method that performs the invoke of the BPULSE REST SERVICE.
	 * 
	 */
	private synchronized void invokeRestService() throws ClientProtocolException, UnsupportedEncodingException, Exception {
		try {
			ExtraResponse<PulsesRS> response = this.restInvoker.postWithProcess(this.bpulseRestURL, this.pulseToSendByRest, new BPulseResponseHandler());
		} catch (Exception e) {
			throw e;
		}
			/*if (response == null || !response.getResponse().getStatus().equals(PulsesRS.StatusType.OK)) {
				throw new ClientProtocolException();
			}*/
	}
	
	/**
	 * Method that performs the call of the delete pulses from PULSESDB method with keysToDelete LIST.
	 * 
	 */
	private synchronized void deletePulseKeysProcessedByRest() throws Exception {
		for(Long keyToDelete : this.keysToDelete) {
			this.dbPulsesRepository.deleteBpulseRQByKey(keyToDelete);
		}
	}
	
	class BPulseResponseHandler implements ResponseHandler <PulsesRS> {

		private String LS = System.getProperty("line.separator");
		
		private Method getNewBuilderMessageMethod(Class<? extends Message> clazz)
				throws NoSuchMethodException {
			Method m =  clazz.getMethod("newBuilder");

			return m;
		}

		private String convertInputStreamToString(InputStream io) {
			StringBuffer sb = new StringBuffer();
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(
						io, "UTF-8"));
				String line = reader.readLine();
				while (line != null) {
					sb.append(line).append(LS);
					line = reader.readLine();
				}
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException("Unable to obtain an InputStream", e);

			}
			return sb.toString();
		}

		@Override
		public PulsesRS handleResponse (final HttpResponse pRestResponse)
				throws ClientProtocolException, IOException {
			PulsesRS generatedPulseRS;
			if (pRestResponse.getStatusLine().getStatusCode() != BPULSE_REST_HTTP_CREATED) {
				throw new ClientProtocolException("HTTP ERROR: " + pRestResponse.getStatusLine().getStatusCode() + " " + pRestResponse.getStatusLine().getReasonPhrase());
			}
			if (RestInvoker.CURRENT_CONTENT_TYPE == TestContentType.PROTOBUF) {
				generatedPulseRS = PulsesRS
						.parseFrom(pRestResponse.getEntity()
								.getContent());
			} else {
				throw new RuntimeException ("Tipo desconocido " + RestInvoker.CURRENT_CONTENT_TYPE);
			}
			
			pRestResponse.getEntity().getContent().close();

			return generatedPulseRS;
		}
			
	}

}

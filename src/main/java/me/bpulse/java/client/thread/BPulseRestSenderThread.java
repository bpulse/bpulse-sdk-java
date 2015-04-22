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
import org.apache.log4j.Logger;

import com.google.protobuf.Message;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.domain.proto.collector.CollectorMessageRS.PulsesRS;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.rest.ExtraResponse;
import me.bpulse.java.client.rest.RestInvoker;
import me.bpulse.java.client.rest.RestInvoker.TestContentType;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REST_HTTP_CREATED;

public class BPulseRestSenderThread implements Runnable{

	private PulsesRQ pulseToSendByRest;
	private IPulsesRepository dbPulsesRepository;
	private List<Long> keysToDelete;
	private RestInvoker restInvoker;
	private String bpulseRestURL;
	private String id;
	final static Logger logger = Logger.getLogger(BPulseRestSenderThread.class);
	
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
			long initTime = Calendar.getInstance().getTimeInMillis();
			invokeRestService();
			logger.info("BPULSE REST TIME: " + (Calendar.getInstance().getTimeInMillis() - initTime));
			deletePulseKeysProcessedByRest();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
			logger.error("BPULSE REST ClientProtocolException: " + e.getMessage());
			releaseCurrentBPulseInProgressKeys();
		} catch (Exception e) {
			logger.error("BPULSE REST ERROR: " + e.getMessage());
			releaseCurrentBPulseInProgressKeys();
		}
	}
	
	private synchronized void releaseCurrentBPulseInProgressKeys() {
		for(Long keyToDelete : this.keysToDelete) {
			this.dbPulsesRepository.releaseBpulseKeyInProgressByKey(keyToDelete);
		}
	}

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
	
	private synchronized void deletePulseKeysProcessedByRest() {
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
			System.out.println("HTTP STATUS: " + pRestResponse.getStatusLine().getStatusCode() + ", " + pRestResponse.getStatusLine().getReasonPhrase());
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

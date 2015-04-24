/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
/**
 * Utilidad para abstraer el envio de credenciales al Collector, la
 * invocaci&oacute;n del servicio y el procesamiento de la respuesta.
 */
public class RestInvoker {

	/** Login del Collector para pruebas del Benchmark .*/
	public static final String TEST_COLLECTOR_LOGIN = "test_collector@";
	
	/** Password del Collector para pruebas del Benchmark .*/
	public static final String TEST_COLLECTOR_PASSWORD = "test_collector_password1";

	public enum TestContentType {
		JSON, PROTOBUF
	}

	/** Tipo de contenido por defecto. */
	public static final TestContentType CURRENT_CONTENT_TYPE = TestContentType.PROTOBUF;
	
	
	/** Cliente para enviar las peticiones REST. Es thread-safe .*/ 
	private static CloseableHttpClient httpclient;
	
	
	/** Contexto de las peticiones REST, guarda por ejemplo las credenciales .*/
	private HttpContext context;
	
	
	/**
	 * Instancia al HttpClient y al HttpContext para que queden listos para ser
	 * usados en las invocaciones.
	 */
	public RestInvoker (final String pUser, final String pPassword, final int invokeTimeout) {
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		credsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(pUser, pPassword));
		
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setDefaultMaxPerRoute(100);
        cm.setMaxTotal(100);

        RequestConfig defaultRequestConfig = RequestConfig.custom()
        	    .setSocketTimeout(invokeTimeout)
        	    .setConnectTimeout(invokeTimeout)
        	    .setConnectionRequestTimeout(invokeTimeout)
        	    .build();
        
        httpclient = HttpClients.custom()
				.setDefaultCredentialsProvider(credsProvider)
				.setConnectionManager(cm)
				.setDefaultRequestConfig(defaultRequestConfig)
				.build();
		context = new BasicHttpContext();
	}

	
	/**
	 * Invoca al servicio REST.
	 * 
	 * @return el resultado de procesar la respuesta del servicio REST con un
	 *         ResponseHandler
	 * @throws IOException 
	 */
	public <T> ExtraResponse <T> postWithProcess(final String pUrl,
			final PulsesRQ pInput,
			final ResponseHandler <T> pResponseHandler)
	throws IOException {		
		T processResponse = null;
		ExtraResponse<T> extraResponse = null;
		long wsTime;
		
		HttpPost httpPost = new HttpPost(pUrl);
		HttpEntity entity;
		
		switch (CURRENT_CONTENT_TYPE) {
		case PROTOBUF:
			httpPost.addHeader("Content-Type", "application/x-protobuf");
			httpPost.addHeader("Accept", "application/x-protobuf");
			
			entity = new ByteArrayEntity(pInput.toByteArray());

			break;
		default:
			throw new RuntimeException ("Tipo de contenido desconocido " + CURRENT_CONTENT_TYPE);
		}

		httpPost.setEntity(entity);
		
		try {
			wsTime = System.currentTimeMillis();
			processResponse = httpclient.execute(httpPost, pResponseHandler,
					context);
			wsTime = System.currentTimeMillis() - wsTime;

			extraResponse = new ExtraResponse<T>(wsTime, processResponse);

		} catch (ClientProtocolException e) {
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}

		return extraResponse;
	}
	
	/**
	 * Invoca al servicio REST.
	 * 
	 * @return el resultado de procesar la respuesta del servicio REST con un
	 *         ResponseHandler
	 */
	public <T> ExtraResponse <T> postWithProcess(final String pUrl,
			final byte[] pInput,
			final ResponseHandler <T> pResponseHandler)
	throws UnsupportedEncodingException {		
		T processResponse = null;
		ExtraResponse<T> extraResponse = null;
		long wsTime;
		
		HttpPost httpPost = new HttpPost(pUrl);
		HttpEntity entity;
		
		switch (CURRENT_CONTENT_TYPE) {
		case PROTOBUF:
			httpPost.addHeader("Content-Type", "application/x-protobuf");
			httpPost.addHeader("Accept", "application/x-protobuf");
			
			entity = new ByteArrayEntity(pInput);

			break;
		default:
			throw new RuntimeException ("Tipo de contenido desconocido " + CURRENT_CONTENT_TYPE);
		}

		httpPost.setEntity(entity);
		
		try {
			wsTime = System.currentTimeMillis();
			processResponse = httpclient.execute(httpPost, pResponseHandler,
					context);
			wsTime = System.currentTimeMillis() - wsTime;

			extraResponse = new ExtraResponse<T>(wsTime, processResponse);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return extraResponse;
	}

}

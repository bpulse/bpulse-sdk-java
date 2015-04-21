package me.bpulse.java.client.rest;

/**
 * Almacena informaci&oacuet;n de respuesta de un petici&oacute;n REST
 * m&aacute;s una serie otros datos que puedan ser de utilidad, como el tiempo
 * que tom&oacute;n la ejecuci&oacute;n de la petici&oacute;n.
 * 
 * @param <T> el tipo de dato de la respuesta obtenida.
 */
public class ExtraResponse <T> {
	
	/** Tiempo que tom&oacute;n la ejecuci&oacute;n de la petici&oacute;n. */
	private long wsTime;
	
	/** Respuesta del servicio REST .*/
	private T response;
	
	
	/**
	 * El constructor permite asignar el valor del tiempo de ejecuci&oacute;n y
	 * la respuesta obtenida.
	 */
	public ExtraResponse(long pWsTime, final T pResponse) {
		wsTime = pWsTime;
		response = pResponse;
	}

	
	/** @return el tiempo que tom&oacute;n la ejecuci&oacute;n de la petici&oacute;n. */
	public long getWsTime() {
		return wsTime;
	}

	
	/** @return la respuesta del servicio REST .*/
	public T getResponse() {
		return response;
	}

}

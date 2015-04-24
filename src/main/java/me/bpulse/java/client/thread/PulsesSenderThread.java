/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.thread;

import javax.sql.rowset.spi.SyncResolver;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class PulsesSenderThread implements Runnable{
	
	private PulsesRQ pulseRQToPersist;
	private IPulsesRepository dbPulsesRepository;
	private String id;
	
	public PulsesSenderThread(String idThread, PulsesRQ pulseToPersist, IPulsesRepository pulsesRepository) {
		this.pulseRQToPersist = pulseToPersist;
		this.dbPulsesRepository = pulsesRepository;
		this.id = idThread;
	}

	public void run() {
		try {
			sendPulseToRepository();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Method that performs the call of the savePulse method of PulsesRepository.
	 * 
	 */
	public synchronized void  sendPulseToRepository() throws Exception {
		this.dbPulsesRepository.savePulse(this.pulseRQToPersist);
	}

}

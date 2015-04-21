package me.bpulse.java.client.thread;

import javax.sql.rowset.spi.SyncResolver;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.pulsesrepository.PulsesRepository;

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
		sendPulseToRepository();
	}
	
	public synchronized void  sendPulseToRepository() {
		this.dbPulsesRepository.savePulse(this.pulseRQToPersist);
	}

}

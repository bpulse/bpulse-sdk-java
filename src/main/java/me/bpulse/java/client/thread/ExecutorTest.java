package me.bpulse.java.client.thread;


import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import java.util.concurrent.ThreadLocalRandom; 
import java.util.concurrent.TimeUnit; 
import java.util.concurrent.atomic.AtomicInteger; 

import me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.Value;
import me.bpulse.java.client.pulsesclient.BPulseJavaClient;
import me.bpulse.java.client.pulsesender.BPulseSender;
import me.bpulse.java.client.pulsesrepository.IPulsesRepository;
import me.bpulse.java.client.pulsesrepository.PulsesRepository;

import org.junit.Test; 
public class ExecutorTest { 
	private static AtomicInteger counter = new AtomicInteger(1); 
	class Task implements Runnable{ 
		@Override public void run() { 
		try{ 
			Integer taskNumber = counter.getAndIncrement(); 
		System.out.println("Executing task "+ taskNumber); 
		Thread.sleep(TimeUnit.SECONDS.toMillis(ThreadLocalRandom.current().nextInt(5))); 
		System.out.println("The task "+taskNumber+ " has finished"); 
		
		BPulseSender client = BPulseSender.getInstance();
		System.out.println("INVOKING THREAD: ");
		String resp = client.sendPulse(createPulses(5, true));
		System.out.println("THREAD: "+ " COMPLETE");
		
		}
		catch(Exception e){ 
			e.printStackTrace(); 
			} } 
		
		private PulsesRQ createPulses(int numPulses, boolean ramdomClient) {
			PulsesRQ request;
			PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
			Pulse.Builder pulse = null;
			pulses.setVersion("0.1");

			String[] clients = new String[1000];
			for (int i = 0; i < 1000; i++) {
				clients[i] = "client-00" + i;
			}

			String[] destinations = new String[1000];
			for (int i = 0; i < 1000; i++) {
				destinations[i] = "dest-XX" + i;
			}

			Random randomGenerator = new Random();
			// Pulse de la lista de pulsos
			for (int k = 0; k < numPulses; k++) {
				pulse = Pulse.newBuilder();
				String PULSE_DEF_ID_AVAILS = "BPULSE"
						+ "_" + "PRUEBAPULSOS" + "-"
						+ "avails";
				pulse.setTypeId(PULSE_DEF_ID_AVAILS);
				pulse.setTime(System.currentTimeMillis());
				pulse.setInstanceId(String.valueOf(k));

				Value.Builder value = null;

				// Valores del pulso

				value = Value.newBuilder();
				value.setName("clientId");
				int randomInt = 0;
				if (ramdomClient) {
					randomInt = randomGenerator.nextInt(999);
				}
				// Agrega un cliente
				value.addValues(clients[randomInt]);
				pulse.addValues(value);

				// Status
				value = Value.newBuilder();
				value.setName("status");
				value.addValues("OK");
				pulse.addValues(value);

				// rsTime
				value = Value.newBuilder();
				value.setName("rsTime");
				value.addValues(String.valueOf(randomGenerator.nextLong()));
				pulse.addValues(value);

				// Destinations
				value = Value.newBuilder();
				value.setName("dest");
				value.addValues(destinations[randomGenerator.nextInt(999)]);
				pulse.addValues(value);

				// nHotels
				value = Value.newBuilder();
				value.setName("nHotels");
				value.addValues(destinations[randomGenerator.nextInt(800)]);
				pulse.addValues(value);

				pulses.addPulse(pulse);
			}

			request = pulses.build();
			//System.out.println(request.getSerializedSize());
			return request;
		}
	
	} 
	
	/*@Test public void testExecutor1() throws InterruptedException{ 
		ExecutorService executor = Executors.newFixedThreadPool(5);//newCachedThreadPool(); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task()); 
		executor.execute(new Task());
		Thread.sleep(5000);
		//executor.shutdown(); //executor.awaitTermination(30, TimeUnit.SECONDS); } }
	}*/
	
	@Test
	public void test2() throws InterruptedException {
		
		Random randomGenerator = new Random();
        ExecutorService threadPool;
        threadPool = Executors.newFixedThreadPool(10);
        BPulseJavaClient client = BPulseJavaClient.getInstance();
        
        //tamano repositorio antes
        IPulsesRepository pulsesRepository = client.getBPulseSender().getPulsesRepository();
        //Object[] keys = pulsesRepository.getSortedbpulseRQMapKeys();
		int before = 0;// pulsesRepository.countBpulsesRQ();
		System.out.println("KEYS GENERATED BEFORE: " + before + " "  + Calendar.getInstance().getTime());
        int nrecords = 60000;
        long initTime = System.currentTimeMillis();
        long globalEndTime = System.currentTimeMillis();
        boolean flagStopTimeInserted = false;
        boolean flagStopTimeProcessed = false;
		for (int i=0; i<nrecords; i++) {
	        BPulseJavaClientThread bpulseJavaClientThread = new BPulseJavaClientThread("THREAD_"+i, createPulses(/*randomGenerator.nextInt(100)*/1, true), client);
	        
	        Thread execThread = new Thread(bpulseJavaClientThread);
	        execThread.start();
	        if ((i+1)%1000 == 0) {
	        	//Thread.sleep(1000);
	        	long init = System.currentTimeMillis();
	        	while ((System.currentTimeMillis() - init) < 500) {
	        		
	        	}
	        	System.out.println("SLEEP TIME: " + (System.currentTimeMillis() - init));
	        }
	        
        }
		/*long init = System.currentTimeMillis();
		while ((System.currentTimeMillis() - init) < 120000) {
    		
    	}*/
		
		int after = before;
		//while((after - before) < nrecords) {
		while(true) {//(globalEndTime - initTime) < 720000) {
			after = pulsesRepository.getInsertedRecords();//pulsesRepository.countBpulsesRQ();
			//System.out.println("INSERTED RECORDS: " + pulsesRepository.getInsertedRecords() + " " + Calendar.getInstance().getTime() + " INSERT AVERAGE MILLIS: " + pulsesRepository.getInsertTimeMillisAverage());
			if (after >= nrecords && !flagStopTimeInserted) {
				System.out.println("INSERTED RECORDS COMPLETED: " + after + " " + Calendar.getInstance().getTime() + " INSERT AVERAGE MILLIS: " + pulsesRepository.getInsertTimeMillisAverage());
				flagStopTimeInserted = true;
				//break;
			}
			if (pulsesRepository.countBpulsesRQ() == 0 && !flagStopTimeProcessed) {
				System.out.println("PROCESSED RECORDS COMPLETED: " + Calendar.getInstance().getTime() + " DELETE AVERAGE MILLIS: " + pulsesRepository.getDeleteTimeMillisAverage() + " GET AVERAGE MILLIS: " + pulsesRepository.getGetTimeMillisAverage() + " GET SORTED KEYS AVERAGE MILLIS: " + pulsesRepository.getSortedKeysTimeMillisAverage());
				flagStopTimeProcessed = true;
				break;
			}
			globalEndTime = System.currentTimeMillis();
			/*if ((globalEndTime - initTime) > 360000) {
				break;
			}*/
		}
		
		after = pulsesRepository.countBpulsesRQ();
		System.out.println("KEYS GENERATED AFTER: " + after);
		System.out.println("INSERTED RECORDS: " + pulsesRepository.getInsertedRecords());
		long endTime = System.currentTimeMillis();
		System.out.println("TOTAL TIME: " + (endTime - initTime));
	}
	
	@Test
	public void test3() {
		
		BPulseJavaClient client = BPulseJavaClient.getInstance();
		IPulsesRepository pulsesRepository = client.getBPulseSender().getPulsesRepository();
		
		for (int i=0; i<1000; i++) {
	        
	        pulsesRepository.savePulse(createPulses(1, true));
	        
        }
		
		Object[] keys = pulsesRepository.getSortedbpulseRQMapKeys();
		for (Object k : keys) {
			System.out.println(k.toString());
		}
		
	}
	
	private PulsesRQ createPulses(int numPulses, boolean ramdomClient) {
		PulsesRQ request;
		PulsesRQ.Builder pulses = PulsesRQ.newBuilder();
		Pulse.Builder pulse = null;
		pulses.setVersion("0.1");

		String[] clients = new String[1000];
		for (int i = 0; i < 1000; i++) {
			clients[i] = "client-00" + i;
		}

		String[] destinations = new String[1000];
		for (int i = 0; i < 1000; i++) {
			destinations[i] = "dest-XX" + i;
		}

		Random randomGenerator = new Random();
		// Pulse de la lista de pulsos
		for (int k = 0; k < numPulses; k++) {
			pulse = Pulse.newBuilder();
			String PULSE_DEF_ID_AVAILS = "bpulse_clienteuno_booking";
			pulse.setTypeId(PULSE_DEF_ID_AVAILS);
			pulse.setTime(System.currentTimeMillis());
			pulse.setInstanceId(String.valueOf(k));

			Value.Builder value = null;

			// Valores del pulso

			value = Value.newBuilder();
			value.setName("clientId");
			int randomInt = 0;
			if (ramdomClient) {
				randomInt = randomGenerator.nextInt(999);
			}
			// Agrega un cliente
			value.addValues(clients[randomInt]);
			pulse.addValues(value);

			// Status
			/*value = Value.newBuilder();
			value.setName("status");
			value.addValues("OK");
			pulse.addValues(value);*/

			// rsTime
			/*value = Value.newBuilder();
			value.setName("rsTime");
			value.addValues(String.valueOf(randomGenerator.nextLong()));
			pulse.addValues(value);*/

			// Destinations
			value = Value.newBuilder();
			value.setName("destinationId");
			value.addValues(destinations[randomGenerator.nextInt(999)]);
			pulse.addValues(value);
			
			value = Value.newBuilder();
			value.setName("residenceTime");
			value.addValues(new Integer(randomGenerator.nextInt(999)).toString());
			pulse.addValues(value);

			// nHotels
			/*value = Value.newBuilder();
			value.setName("nHotels");
			value.addValues(destinations[randomGenerator.nextInt(800)]);
			pulse.addValues(value);*/

			pulses.addPulse(pulse);
		}

		request = pulses.build();
		//System.out.println(request.getSerializedSize());
		return request;
	}
	
}

class BPulseJavaClientThread implements Runnable {
	
	private PulsesRQ pulseToSend;
	private String id;
	private BPulseJavaClient bpulseClient;
	
	public BPulseJavaClientThread(String threadId, PulsesRQ pulsesRQToSend, BPulseJavaClient pBpulseClient) {
		this.pulseToSend = pulsesRQToSend;
		this.id = threadId;
		this.bpulseClient = pBpulseClient;
	}

	public void run() {
		// TODO Auto-generated method stub
		//System.out.println("INVOKING THREAD: " + id);
		try {
			String resp = this.bpulseClient.sendPulse(this.pulseToSend);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("THREAD: " + id + " COMPLETE");// resp: " + resp);
	}
	
	
	
}

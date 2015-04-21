package me.bpulse.java.client.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;






import me.bpulse.java.client.properties.PropertiesManager;

import org.apache.log4j.Logger;

public class ThreadPoolManager {
	
	/** Log de la clase. */
	private static Logger logger = Logger.getLogger(ThreadPoolManager.class);
	private ExecutorService threadPool;
	
	public ThreadPoolManager(int pPoolSize) {
		/*threadPool = new ThreadPoolExecutor(pPoolSize,
				pMaxPoolSize, pKeepAliveTimeMilliseconds, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(), new RejectedExecution());*/
		
		this.threadPool = Executors.newFixedThreadPool(pPoolSize);
		
	}
	
	public ExecutorService getThreadPoolExecutor() {
		return threadPool;
	}
	
	/*class RejectedExecution implements RejectedExecutionHandler {
		/**
		 * Metodo invacado por el Pool de Hilos cuando alguna tarea no puede ser
		 * ejecutada o incluida en la cola de tareas pendientes.
		 * 
		 * @param task
		 *            Tarea que no puede ser gestionada por el pool.
		 * @param executor
		 *            El pool de hilos que ha excluido la tarea,
		 * @since 1.0.0
		 
		public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
			logger.warn("rejectedExecution!! [Task= " + task.toString());
		}
	}*/

}

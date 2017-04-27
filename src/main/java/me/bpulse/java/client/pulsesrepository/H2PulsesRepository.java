/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.pulsesrepository;

import java.io.File;
import java.io.InputStream;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.rowset.serial.SerialBlob;

import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.java.client.properties.PropertiesManager;

import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REPOSITORY_NAME;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_REPOSITORY_USER;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_STATUS_INPROGRESS;
import static me.bpulse.java.client.common.BPulseConstants.BPULSE_STATUS_PENDING;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_0;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_1;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_2;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_3;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_5;
import static me.bpulse.java.client.common.BPulseConstants.COMMON_NUMBER_60;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
public class H2PulsesRepository implements IPulsesRepository {

	private JdbcConnectionPool connectionPool;
	private int insertedRecords = COMMON_NUMBER_0;
	private long insertTimeMillisAverage = COMMON_NUMBER_0;
	private long deleteTimeMillisAverage = COMMON_NUMBER_0;
	private long getTimeMillisAverage = COMMON_NUMBER_0;
	private long sortedKeysTimeMillisAverage = COMMON_NUMBER_0;
	private int limitNumberPulsesToReadFromDb = COMMON_NUMBER_0;
	private Map<Integer,String> bpulseTableInProgressMap;
	private String dbPath;
	private File dbFile;
	final static Logger logger = LoggerFactory.getLogger("bpulseLogger");
	
	public H2PulsesRepository(int maxNumberPulsesToProcessByTimer) throws Exception {
		
		dbPath = PropertiesManager.getProperty("bpulse.client.pulsesRepositoryDBPath");
		if (dbPath == null) {
			dbPath =  "~";
		}
		//jdbc:h2:~/test
		try {
		connectionPool = JdbcConnectionPool.create("jdbc:h2:"+dbPath+"/"+BPULSE_REPOSITORY_NAME + ";DB_CLOSE_ON_EXIT=FALSE", BPULSE_REPOSITORY_USER, "");
		String initialPoolSizeSendPulses = "" + COMMON_NUMBER_5;//PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES);
		int pPoolSizeSendPulses = COMMON_NUMBER_0;
		if (initialPoolSizeSendPulses != null) {
			pPoolSizeSendPulses = Integer.parseInt(initialPoolSizeSendPulses);
		} else {
			pPoolSizeSendPulses = COMMON_NUMBER_5;
		}
		
		connectionPool.setMaxConnections(pPoolSizeSendPulses);
		connectionPool.setLoginTimeout(COMMON_NUMBER_5);//TODO DEFINIR TIEMPO MAXIMO DE ESPERA
		
		dbFile = new File(dbPath+"/"+BPULSE_REPOSITORY_NAME+".mv.db");
		
		} catch (Exception e) {
			logger.error("FAILED TO CREATE PULSES DATABASE: PLEASE CHECK YOUR PULSESREPOSITORYPATH IN THE BPULSE PROPERTIES FILE.");
			throw e;
		}
		
		this.limitNumberPulsesToReadFromDb = maxNumberPulsesToProcessByTimer;
		logger.debug("PREPARING TO CREATE PULSES DATABASE.");
		try {
			Connection conn = connectionPool.getConnection();
			PreparedStatement createPreparedStatement = null;
			for (int i=COMMON_NUMBER_0; i < COMMON_NUMBER_60; i++) {
				String CreateQuery = "CREATE TABLE BPULSE_PULSESRQ_" + i + "(pulserq_id BIGINT primary key, pulserq_object BLOB, pulserq_status varchar(2))";
				createPreparedStatement = conn.prepareStatement(CreateQuery);
	            createPreparedStatement.executeUpdate();
			}
            createPreparedStatement.close();
            conn.close();
		} catch (SQLException e) {
			logger.debug("PULSES DATABASE ALREADY EXISTS.");
		}

		bpulseTableInProgressMap = new HashMap<Integer,String>();
		
	}

	/**
	 * Method that saves the sent pulse in the PULSESDB.
	 * 
	 * @param pPulsesRQ The Pulse in Protobuf format.
	 */
	@Override
	public synchronized void savePulse(PulsesRQ pPulsesRQ) throws Exception {
		String InsertQuery = "INSERT INTO BPULSE_PULSESRQ" + "(pulserq_id, pulserq_object, pulserq_status) values" + "(?,?,?)";
		Connection conn;
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		long key = System.currentTimeMillis()+additionalPulseId;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement insertPreparedStatement = null;
			insertPreparedStatement = conn.prepareStatement(InsertQuery);
	        insertPreparedStatement.setLong(COMMON_NUMBER_1, key);
	        Blob blob = new SerialBlob(pPulsesRQ.toByteArray());
	        insertPreparedStatement.setBlob(COMMON_NUMBER_2, blob);
	        insertPreparedStatement.setString(COMMON_NUMBER_3, BPULSE_STATUS_PENDING);
	        insertPreparedStatement.executeUpdate();
	        conn.commit();
	        insertPreparedStatement.close();
	        conn.close();
	        insertedRecords++;
			this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO SAVE PULSE: ", e);
			throw e;
		}

	}
	
	/**
	 * Method that saves the sent pulse in the PULSESDB.
	 * 
	 * @param pPulsesRQ The Pulse in Protobuf format.
	 */
	@Override
	public synchronized void savePulse(PulsesRQ pPulsesRQ, int tableIndex) throws Exception {
		String InsertQuery = "INSERT INTO BPULSE_PULSESRQ_" + tableIndex + "(pulserq_id, pulserq_object, pulserq_status) values" + "(?,?,?)";
		Connection conn;
		long initTime = Calendar.getInstance().getTimeInMillis();
		Random random = new Random();
		long additionalPulseId = Math.abs(random.nextLong());
		long key = System.currentTimeMillis()+additionalPulseId;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement insertPreparedStatement = null;
			insertPreparedStatement = conn.prepareStatement(InsertQuery);
	        insertPreparedStatement.setLong(COMMON_NUMBER_1, key);
	        Blob blob = new SerialBlob(pPulsesRQ.toByteArray());
	        insertPreparedStatement.setBlob(COMMON_NUMBER_2, blob);
	        insertPreparedStatement.setString(COMMON_NUMBER_3, BPULSE_STATUS_PENDING);
	        insertPreparedStatement.executeUpdate();
	        conn.commit();
	        insertPreparedStatement.close();
	        conn.close();
	        insertedRecords++;
			this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO SAVE PULSE: ", e);
			throw e;
		}

	}

	/**
	 * Method that performs the query from all pulse keys pending to send to BPULSE REST SERVICE.
	 * 
	 * @return Object[] with all pulse keys found.
	 */
	@Override
	public synchronized Object[] getSortedbpulseRQMapKeys() throws Exception{
		long initTime = Calendar.getInstance().getTimeInMillis();
		Connection conn;
		List<Object> resp = new ArrayList<Object>();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select * from BPULSE_PULSESRQ where pulserq_status = 'P' order by pulserq_id LIMIT ?";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
			selectPreparedStatement.setInt(COMMON_NUMBER_1, this.limitNumberPulsesToReadFromDb);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	        	resp.add(rs.getLong(COMMON_NUMBER_1));
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
			this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE PENDING PULSES LIST: ", e);
			throw e;
		}
		
		
		return resp.toArray();
	}
	
	/**
	 * Method that performs the query from all pulse keys pending to send to BPULSE REST SERVICE.
	 * 
	 * @return Object[] with all pulse keys found.
	 */
	@Override
	public synchronized Object[] getSortedbpulseRQMapKeys(int tableIndex) throws Exception{
		long initTime = Calendar.getInstance().getTimeInMillis();
		Connection conn;
		List<Object> resp = new ArrayList<Object>();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select * from BPULSE_PULSESRQ_" + tableIndex + " where pulserq_status = 'P' order by pulserq_id LIMIT ?";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
			selectPreparedStatement.setInt(COMMON_NUMBER_1, this.limitNumberPulsesToReadFromDb);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	        	resp.add(rs.getLong(COMMON_NUMBER_1));
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
			this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE PENDING PULSES LIST: ", e);
			throw e;
		}
		
		
		return resp.toArray();
	}

	/**
	 * Method that obtains the associated pulsesRQ to the selected key from PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	@Override
	public synchronized PulsesRQ getBpulseRQByKey(Long pKey) throws Exception{
		Connection conn;
		PulsesRQ resp = null;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select * from BPULSE_PULSESRQ where pulserq_id=?";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
			selectPreparedStatement.setLong(COMMON_NUMBER_1, pKey);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	        	Blob obj = rs.getBlob(COMMON_NUMBER_2);
	            resp = PulsesRQ.parseFrom(obj.getBytes(1L, (int)obj.length()));
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE PULSE BY KEY: ", e);
			throw e;
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			logger.error("FAILED TO FORMAT PULSE TO PROTOBUF STRUCTURE: ", e);
			throw e;
		}
		return resp;
	}
	
	/**
	 * Method that obtains the associated pulsesRQ to the selected key from PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	@Override
	public synchronized PulsesRQ getBpulseRQByKey(Long pKey, int tableIndex) throws Exception{
		Connection conn;
		PulsesRQ resp = null;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select * from BPULSE_PULSESRQ_" + tableIndex + " where pulserq_id=?";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
			selectPreparedStatement.setLong(COMMON_NUMBER_1, pKey);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	        	Blob obj = rs.getBlob(COMMON_NUMBER_2);
	            resp = PulsesRQ.parseFrom(obj.getBytes(1L, (int)obj.length()));
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE PULSE BY KEY: ", e);
			throw e;
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			logger.error("FAILED TO FORMAT PULSE TO PROTOBUF STRUCTURE: ", e);
			throw e;
		}
		return resp;
	}

	/**
	 * Method that performs the deletion of the associated pulsesRQ to the selected key.
	 * @param pKey The pulses key.
	 */
	@Override
	public synchronized void deleteBpulseRQByKey(Long pKey) throws Exception{
		String deleteQuery = "DELETE FROM BPULSE_PULSESRQ" + " WHERE pulserq_id=?";
		Connection conn;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			PreparedStatement deletePreparedStatement = null;
			deletePreparedStatement = conn.prepareStatement(deleteQuery);
			deletePreparedStatement.setLong(COMMON_NUMBER_1, pKey);
			deletePreparedStatement.executeUpdate();
	        conn.commit();
	        deletePreparedStatement.close();
	        conn.close();
	        this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO DELETE PULSE: ", e);
			throw e;
		}

	}
	
	/**
	 * Method that performs the deletion of the associated pulsesRQ to the selected key.
	 * @param pKey The pulses key.
	 */
	@Override
	public synchronized void truncateBpulseRQByTableIndex(int tableIndex) throws Exception{
		String deleteQuery = "TRUNCATE TABLE BPULSE_PULSESRQ_" + tableIndex;
		Connection conn;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			PreparedStatement deletePreparedStatement = null;
			deletePreparedStatement = conn.prepareStatement(deleteQuery);
			deletePreparedStatement.executeUpdate();
	        conn.commit();
	        deletePreparedStatement.close();
	        conn.close();
	        this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO TRUNCATE TABLE: BPULSE_PULSESRQ_" + tableIndex, e);
			throw e;
		}

	}

	/**
	 * Method that counts the current pulsesRQ in the PULSESDB.
	 * @return The pulses amount in the PULSESDB.
	 */
	@Override
	public int countBpulsesRQ() throws Exception{
		Connection conn;
		int resp = COMMON_NUMBER_0;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select count(*) from BPULSE_PULSESRQ";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	            resp = rs.getInt(COMMON_NUMBER_1);
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE CURRENT PULSES NUMBER IN DB: ", e);
			throw e;
		}
		
		return resp;
	}
	
	/**
	 * Method that counts the current pulsesRQ in the PULSESDB.
	 * @return The pulses amount in the PULSESDB.
	 */
	@Override
	public int countBpulsesRQ(int tableIndex) throws Exception{
		Connection conn;
		int resp = COMMON_NUMBER_0;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select count(*) from BPULSE_PULSESRQ_" + tableIndex;
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	            resp = rs.getInt(COMMON_NUMBER_1);
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			logger.error("FAILED TO GET THE CURRENT PULSES NUMBER IN DB: ", e);
			throw e;
		}
		
		return resp;
	}

	/**
	 * Method that performs the change of the pulsesRQ State from PENDING to INPROGRESS in the PULSESDB.
	 * @param pKey The pulses key.
	 */
	@Override
	public synchronized void markBpulseKeyInProgress(Long pKey) throws Exception{
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ? WHERE pulserq_id=?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_INPROGRESS);
			updatePreparedStatement.setLong(COMMON_NUMBER_2, pKey);
			updatePreparedStatement.executeUpdate();
	        conn.commit();
	        updatePreparedStatement.close();
	        conn.close();
		} catch (SQLException e) {
			logger.error("FAILED TO UPDATE THE PULSE STATE FROM PENDING TO INPROGRESS: ", e);
			throw e;
		}

	}
	
	/**
	 * Method that performs the change of the pulsesRQ State from PENDING to INPROGRESS in the PULSESDB.
	 * @param pKey The pulses key.
	 */
	@Override
	public synchronized void markBpulseTableInProgress(int tableIndex) throws Exception{
		
		try {
			bpulseTableInProgressMap.put(tableIndex, BPULSE_STATUS_INPROGRESS);
		} catch (Exception e) {
			logger.error("FAILED TO UPDATE THE TABLE " + tableIndex + " STATE TO INPROGRESS: ", e);
			throw e;
		}

	}

	/**
	 * Method that counts the number of pulses with PENDING state in the PULSESDB.
	 * @return The PENDING pulses amount.
	 */
	@Override
	public int countMarkBpulseKeyInProgress() throws Exception{
		
		Connection conn;
		int resp = COMMON_NUMBER_0;
		try {
			conn = connectionPool.getConnection();
			String SelectQuery = "select count(*) from BPULSE_PULSESRQ where pulserq_status = 'I'";
			PreparedStatement selectPreparedStatement = conn.prepareStatement(SelectQuery);
	        ResultSet rs = selectPreparedStatement.executeQuery();
	        while (rs.next()) {
	            resp = rs.getInt(COMMON_NUMBER_1);
	        }
	        rs.close();
	        selectPreparedStatement.close();
	        conn.close();
		} catch (SQLException e) {
			logger.error("FAILED TO GET PULSES INPROGRESS COUNT: ", e);
			throw e;
		}
		
		return resp;
		
	}
	
	/**
	 * Method that counts the number of pulses with PENDING state in the PULSESDB.
	 * @return The PENDING pulses amount.
	 */
	@Override
	public int countMarkBpulseTableInProgress() throws Exception{
		
		int resp = COMMON_NUMBER_0;
		try {
			resp = bpulseTableInProgressMap.size();
		} catch (Exception e) {
			logger.error("FAILED TO GET PULSES INPROGRESS COUNT: ", e);
			throw e;
		}
		
		return resp;
		
	}

	/**
	 * Method that performs the change of the pulsesRQ State from INPROGRESS to PENDING in the PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	@Override
	public synchronized void releaseBpulseKeyInProgressByKey(Long pKey) throws Exception{
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ? WHERE pulserq_id=?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_PENDING);
			updatePreparedStatement.setLong(COMMON_NUMBER_2, pKey);
			updatePreparedStatement.executeUpdate();
	        conn.commit();
	        updatePreparedStatement.close();
	        conn.close();
		} catch (SQLException e) {
			logger.error("FAILED TO UPDATE THE PULSE STATE FROM INPROGRESS TO PENDING: ", e);
			throw e;
		}

	}
	
	/**
	 * Method that performs the change of the pulsesRQ State from INPROGRESS to PENDING in the PULSESDB.
	 * @param pKey The pulses key.
	 * @return PulsesRQ The pulse to process.
	 */
	@Override
	public synchronized void releaseBpulseTableInProgress(int tableIndex) throws Exception{
		
		try {
			if(!bpulseTableInProgressMap.isEmpty()) {
				bpulseTableInProgressMap.remove(tableIndex);
			}
		} catch (Exception e) {
			logger.error("FAILED TO RELEASE THE TABLE " + tableIndex, e);
			throw e;
		}

	}
	
	public boolean isAvailableBpulseTable(int tableIndex) throws Exception{
		
		boolean resp = true;
		
		try {
			if(!bpulseTableInProgressMap.isEmpty() && bpulseTableInProgressMap.get(tableIndex) != null) {
				resp = false;
			}
		} catch (Exception e) {
			logger.error("FAILED TO VALIDATE THE TABLE AVAILABILITY " + tableIndex, e);
			throw e;
		}
		
		return resp;
	}
	
	/**
	 * Method that performs the change of the pulsesRQ State from INPROGRESS to PENDING for all the pulses in the PULSESDB.
	 */
	public void convertAllBpulseKeyInProgressToPending() throws Exception{
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_PENDING);
			updatePreparedStatement.executeUpdate();
	        conn.commit();
	        updatePreparedStatement.close();
	        conn.close();
		} catch (SQLException e) {
			logger.error("FAILED TO MASSIVE UPDATE THE PULSES STATE FROM INPROGRESS TO PENDING: ", e);
			throw e;
		}

	}

	@Override
	public int getInsertedRecords() {
		return insertedRecords;
	}

	@Override
	public long getInsertTimeMillisAverage() {
		return insertTimeMillisAverage;
	}

	@Override
	public long getDeleteTimeMillisAverage() {
		return deleteTimeMillisAverage;
	}

	@Override
	public long getGetTimeMillisAverage() {
		return getTimeMillisAverage;
	}

	@Override
	public long getSortedKeysTimeMillisAverage() {
		return sortedKeysTimeMillisAverage;
	}
	
	/**
	 * Method that gets the current size of the PULSESDB file.
	 * @return The PULSESDB file size in bytes.
	 */
	@Override
	public long getDBSize() {
        return dbFile.length();
	}

}

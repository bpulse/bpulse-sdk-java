package me.bpulse.java.client.pulsesrepository;

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

import org.fusesource.lmdbjni.EntryIterator;
import org.h2.jdbcx.JdbcConnectionPool;

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

public class H2PulsesRepository implements IPulsesRepository {

	private JdbcConnectionPool connectionPool;
	private int insertedRecords = COMMON_NUMBER_0;
	private long insertTimeMillisAverage = COMMON_NUMBER_0;
	private long deleteTimeMillisAverage = COMMON_NUMBER_0;
	private long getTimeMillisAverage = COMMON_NUMBER_0;
	private long sortedKeysTimeMillisAverage = COMMON_NUMBER_0;
	private int limitNumberPulsesToReadFromDb = COMMON_NUMBER_0;
	private Map<String,String> bpulseRQInProgressMap;
	
	public H2PulsesRepository(int maxNumberPulsesToProcessByTimer) {
		
		String dbPath = PropertiesManager.getProperty("bpulse.client.pulsesRepositoryDBPath");
		if (dbPath == null) {
			dbPath =  "~";
		}
		//jdbc:h2:~/test
		connectionPool = JdbcConnectionPool.create("jdbc:h2:"+dbPath+"/"+BPULSE_REPOSITORY_NAME, BPULSE_REPOSITORY_USER, "");
		String initialPoolSizeSendPulses = PropertiesManager.getProperty(BPULSE_PROPERTY_NUMBER_THREADS_SEND_PULSES);
		int pPoolSizeSendPulses = COMMON_NUMBER_0;
		if (initialPoolSizeSendPulses != null) {
			pPoolSizeSendPulses = Integer.parseInt(initialPoolSizeSendPulses);
		} else {
			pPoolSizeSendPulses = COMMON_NUMBER_5;
		}
		
		connectionPool.setMaxConnections(pPoolSizeSendPulses);
		
		this.limitNumberPulsesToReadFromDb = maxNumberPulsesToProcessByTimer;
		
		try {
			Connection conn = connectionPool.getConnection();
			PreparedStatement createPreparedStatement = null;
			String CreateQuery = "CREATE TABLE BPULSE_PULSESRQ(pulserq_id BIGINT primary key, pulserq_object BLOB, pulserq_status varchar(2))";
			createPreparedStatement = conn.prepareStatement(CreateQuery);
            createPreparedStatement.executeUpdate();
            createPreparedStatement.close();
            conn.close();
            
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		convertAllBpulseKeyInProgressToPending();

		bpulseRQInProgressMap = new HashMap<String,String>();
		
	}

	@Override
	public synchronized void savePulse(PulsesRQ pPulsesRQ) {
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
	        insertPreparedStatement.close();
	        conn.commit();
	        conn.close();
	        insertedRecords++;
			this.insertTimeMillisAverage = this.insertTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public synchronized Object[] getSortedbpulseRQMapKeys() {
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
	        selectPreparedStatement.close();
	        conn.close();
			this.sortedKeysTimeMillisAverage = this.sortedKeysTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		return resp.toArray();
	}

	@Override
	public EntryIterator getIterableEntriesBpulseRQMapKeys() {
		return null;
	}

	@Override
	public synchronized PulsesRQ getBpulseRQByKey(Long pKey) {
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
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		return resp;
	}

	@Override
	public byte[] getSerializedBpulseRQByKey(String pKey) {
		return null;
	}

	@Override
	public synchronized void deleteBpulseRQByKey(Long pKey) {
		String deleteQuery = "DELETE FROM BPULSE_PULSESRQ" + " WHERE pulserq_id=?";
		Connection conn;
		long initTime = Calendar.getInstance().getTimeInMillis();
		try {
			conn = connectionPool.getConnection();
			PreparedStatement deletePreparedStatement = null;
			deletePreparedStatement = conn.prepareStatement(deleteQuery);
			deletePreparedStatement.setLong(COMMON_NUMBER_1, pKey);
			deletePreparedStatement.executeUpdate();
			deletePreparedStatement.close();
	        conn.commit();
	        conn.close();
	        //bpulseRQInProgressMap.remove(pKey);
	        this.deleteTimeMillisAverage = this.deleteTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public int countBpulsesRQ() {
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
	        selectPreparedStatement.close();
	        conn.close();
	        this.getTimeMillisAverage = this.getTimeMillisAverage + (Calendar.getInstance().getTimeInMillis() - initTime);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return resp;
	}

	@Override
	public synchronized void markBpulseKeyInProgress(Long pKey) {
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ? WHERE pulserq_id=?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_INPROGRESS);
			updatePreparedStatement.setLong(COMMON_NUMBER_2, pKey);
			updatePreparedStatement.executeUpdate();
			updatePreparedStatement.close();
	        conn.commit();
	        conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//bpulseRQInProgressMap.put(pKey, BPULSE_STATUS_INPROGRESS);

	}

	@Override
	public synchronized String getMarkedBPulseKeyInProgress(String pKey) {
		return bpulseRQInProgressMap.get(pKey);
	}

	@Override
	public int countMarkBpulseKeyInProgress() {
		
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
	        selectPreparedStatement.close();
	        conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return resp;
		
	}

	@Override
	public synchronized void releaseBpulseKeyInProgressByKey(Long pKey) {
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ? WHERE pulserq_id=?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_PENDING);
			updatePreparedStatement.setLong(COMMON_NUMBER_2, pKey);
			updatePreparedStatement.executeUpdate();
			updatePreparedStatement.close();
	        conn.commit();
	        conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public void convertAllBpulseKeyInProgressToPending() {
		
		String updateQuery = "UPDATE BPULSE_PULSESRQ SET pulserq_status = ?";
		Connection conn;
		try {
			conn = connectionPool.getConnection();
			PreparedStatement updatePreparedStatement = null;
			updatePreparedStatement = conn.prepareStatement(updateQuery);
			updatePreparedStatement.setString(COMMON_NUMBER_1, BPULSE_STATUS_PENDING);
			updatePreparedStatement.executeUpdate();
			updatePreparedStatement.close();
	        conn.commit();
	        conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
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

	@Override
	public void initTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void endTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public PulsesRQ getBpulseRQByKey(String pKey) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void deleteBpulseRQByKey(String pKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void markBpulseKeyInProgress(String pKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseBpulseKeyInProgressByKey(String pKey) {
		// TODO Auto-generated method stub
		
	}

}

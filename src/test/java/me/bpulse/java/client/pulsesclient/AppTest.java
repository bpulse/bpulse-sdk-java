package me.bpulse.java.client.pulsesclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.Pulse;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.PulsesRQ;
import me.bpulse.domain.proto.collector.CollectorMessageRQ.Value;
import me.bpulse.java.client.dto.AttributeDto;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    public void testSendPulse()throws Exception{
    	String[] names= new String[]{"nombre","estado","descripcion","XMLResponse","XMLRequest"};
    	PulsesRQ.Builder rqBuild= PulsesRQ.newBuilder();
    	rqBuild.setVersion(String.valueOf(new Date().getTime()));
    	for (int i = 0; i < 10; i++) {
    		Pulse.Builder pulseBuilder= Pulse.newBuilder();
    		pulseBuilder.setInstanceId(String.valueOf(i));
    		pulseBuilder.setTime(new Date().getTime());
    		pulseBuilder.setTypeId("type"+i);
    		for (int j = 0; j < names.length; j++) {
				String name = names[j];
				Value.Builder ValueBuilder= Value.newBuilder();
				ValueBuilder.setName(name+i);
				if(!(name+i).equals("estado0")){
					ValueBuilder.addValues(name+i+" ["+i+"] ["+j+"]");
				}
				pulseBuilder.addValues(ValueBuilder);
			}
    		rqBuild.addPulse(pulseBuilder);
		}
    	
//		BPulseJavaClient client= BPulseJavaClient.getInstance();
		List<AttributeDto> listLong= new ArrayList<AttributeDto>();
		listLong.add(new AttributeDto("type0",Arrays.asList(new String[]{"estado0","nombre0"})));
		listLong.add(new AttributeDto("type1",Arrays.asList(new String[]{"nombre1"})));
		listLong.add(new AttributeDto("type2",Arrays.asList(new String[]{"estado2","nombre2"})));
		List<AttributeDto> listTrace= new ArrayList<AttributeDto>();
		listTrace.add(new AttributeDto("type0",Arrays.asList(new String[]{"XMLRequest0"})));
		listTrace.add(new AttributeDto("type1",Arrays.asList(new String[]{"XMLResponse1","descripcion1"})));
		listTrace.add(new AttributeDto("type2",Arrays.asList(new String[]{"XMLRequest2"})));
		listTrace.add(new AttributeDto("type3",Arrays.asList(new String[]{"XMLRequest3"})));
		listTrace.add(new AttributeDto("type4",Arrays.asList(new String[]{"XMLRequest4"})));
		listTrace.add(new AttributeDto("type5",Arrays.asList(new String[]{"XMLRequest5"})));
		listTrace.add(new AttributeDto("type6",Arrays.asList(new String[]{"XMLRequest6"})));
//		client.sendPulse(rqBuild.build(), listLong, listTrace);
    	
    }
    
}

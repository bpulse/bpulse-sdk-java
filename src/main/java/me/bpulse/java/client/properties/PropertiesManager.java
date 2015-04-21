package me.bpulse.java.client.properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_CONFIG_FILE;

public class PropertiesManager {
	
	static Properties prop = null;
	
	static {
		
		prop = new Properties();
		InputStream input = null;
		
		try {
			
			String propertiesPath = System.getProperty(BPULSE_PROPERTY_CONFIG_FILE);
			
			input = new FileInputStream(propertiesPath);
	 
			// load a properties file
			prop.load(input);
	 
			// get the property value and print it out
	 
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public static String getProperty(String propertyName) {
		if (prop != null) {
			return prop.getProperty(propertyName);
		} else {
			return null;
		}
	}

}

/**
 *  @Copyright (c) BPulse - http://www.bpulse.me
 */
package me.bpulse.java.client.properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static me.bpulse.java.client.common.BPulseConstants.BPULSE_PROPERTY_CONFIG_FILE;

/**
 * @author BPulse team
 * 
 * @Copyright (c) BPulse - http://www.bpulse.me
 */
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
	
	/**
	 * Method that gets the associated value to the specified propertyName.
	 * 
	 * @param propertyName The declared property name in the config.properties file.
	 * @return Associated value to the property name.
	 */
	public static String getProperty(String propertyName) {
		if (prop != null) {
			return prop.getProperty(propertyName);
		} else {
			return null;
		}
	}

}

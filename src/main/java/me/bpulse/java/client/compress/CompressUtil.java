/**
 * @Copyright &copy; BPulse - http://www.bpulse.io
 * @CreationDate 23 de nov. de 2016 - BPulse-Team
 */
package me.bpulse.java.client.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import me.bpulse.java.client.common.BPulseConstants;

/**
 * @author BPulse Team <br/> 
 * 		   Copyright &copy; BPulse - http://www.bpulse.io
 * @class CompressUtil
 * @date 23 de nov. de 2016
 */
public class CompressUtil {

	/**
	 * Compress text
	 * @param val - value to compress
	 * @return compress text
	 * @throws IOException
	 */
	public static String compress(String val) throws IOException {
		if (val == null || val.length() == 0) {
			return val;
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(val.getBytes(BPulseConstants.CHARSET_UTF8));
		gzip.close();
		String outStr = out.toString(BPulseConstants.CHARSET_ISO88591);
		return outStr;
	}
}

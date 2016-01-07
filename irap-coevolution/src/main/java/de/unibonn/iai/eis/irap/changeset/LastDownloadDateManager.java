/**
 * 
 */
package de.unibonn.iai.eis.irap.changeset;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Kemele M. Endris
 *
 */

/**
 * Created by IntelliJ IDEA.
 * User: Morsey
 * Date: Jul 28, 2010
 * Time: 6:26:07 PM
 * This class is responsible for reading and writing the response dates to files, in order to enable resume starting
 * from the last working point both for live extraction and for mapping update
 */
public final class LastDownloadDateManager {

    private static final Logger logger = LoggerFactory.getLogger(LastDownloadDateManager.class);

    private LastDownloadDateManager() {
    }

    public static String getLastDownloadDate(String strFileName) {
        String strLastResponseDate = getFileAsString(strFileName).trim();

        if (strLastResponseDate.isEmpty()) {
            //throw new RuntimeException("Cannot read latest download date from " + strFileName);
        	File f = new File(strFileName);
        	if(!f.exists()){
        		try{
        			f.createNewFile();
        		}catch(Exception e){
        			e.printStackTrace();
        		}
        	}
        }

        return strLastResponseDate;

    }

    public static void writeLastDownloadDate(String strFileName, String strLastResponseDate) {

        try  {
        	OutputStreamWriter osWriter = new OutputStreamWriter(new FileOutputStream(strFileName), "UTF8");
            osWriter.write(strLastResponseDate);
            osWriter.flush();
            osWriter.close();
        } catch (IOException e) {
            logger.warn("Last download date cannot be written to file : " + strLastResponseDate, e);
        }
    }
    public static String getFileAsString(String filename) {
        StringBuilder str = new StringBuilder();

        try  {
        	InputStreamReader in = new InputStreamReader(new FileInputStream(filename), "UTF-8");
            int ch;
            while ((ch = in.read()) != -1) {
                str.append((char) ch);
            }
            in.close();
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File " + filename + " not found!", e);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("UnsupportedEncodingException: ", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("IOException in file " + filename, e);
        }

        return str.toString();
    }
}
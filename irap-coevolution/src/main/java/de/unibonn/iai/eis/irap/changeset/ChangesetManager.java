/**
 * 
 */
package de.unibonn.iai.eis.irap.changeset;

import java.io.File;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import de.unibonn.iai.eis.irap.sync.SyncManager;
import de.unibonn.iai.eis.irap.sync.SynchronizationStrategy;


/**
 * @author Kemele M. Endris
 *
 */
public class ChangesetManager {

	//private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ChangesetManager.class);
		
	public final String LAST_DOWNLOAD = "lastDownloadDate.dat";
	
	public static final String EXTENSION_ADDED_NT =  ".added.nt";
	public static final String EXTENSION_REMOVED_NT =  ".removed.nt";
	public static final int ERRORS_TO_ADVANCE = 5000;
	
	String sourceFolder = "./src_chg";
	String targetFolder = "./target_chg";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new ChangesetManager().begin();
	}
	
	public void begin(){		
		File sourceChanges = new File(sourceFolder);
		File targetChanges = new File(targetFolder);
		
		if(sourceChanges.isDirectory() && targetChanges.isDirectory()){
			
			String lastDownload = LastDownloadDateManager.getLastDownloadDate(LAST_DOWNLOAD);
			
			ChangesetCounter remoteCounter = new HourlyChangesetCounter("2015-11-01-00-000000");
			ChangesetCounter currentCounter = new HourlyChangesetCounter(lastDownload);
	       // currentCounter.incrementCount(); // move to next patch (this one is already applied)
	        
	        int missing_urls = 0;
	        int processedFiles = 0;
	      
	        //TODO: run this in a different thread. Otherwise other changesets folders will not be visited
	        while(true){	        	
	        	String srcAddedTriplesURL =  sourceFolder+'/'+currentCounter.getFormattedFilePath() + EXTENSION_ADDED_NT;
            	String srcDeletedTriplesURL =  sourceFolder+'/'+currentCounter.getFormattedFilePath() + EXTENSION_REMOVED_NT;
            	File srcAddedchanges = new File(srcAddedTriplesURL);
            	File srcDeletedchanges = new File(srcDeletedTriplesURL);
            	
            	String targetAddedTriplesURL =  targetFolder+'/'+currentCounter.getFormattedFilePath() + EXTENSION_ADDED_NT;
            	String targetDeletedTriplesURL =  targetFolder+'/'+currentCounter.getFormattedFilePath() + EXTENSION_REMOVED_NT;
            	File targetAddedchanges = new File(targetAddedTriplesURL);
            	File targetDeletedchanges = new File(targetDeletedTriplesURL);
            	
            	
    			if(!srcAddedchanges.exists() && !srcDeletedchanges.exists() && !targetAddedchanges.exists() && !targetDeletedchanges.exists()){
    				 missing_urls++;    
            		 if (missing_urls >= ERRORS_TO_ADVANCE) {
						// advance hour / day / month or year
						currentCounter.advanceCounter();	
						missing_urls = 0;
					}else{
						currentCounter.incrementCount();
					}
            		if(currentCounter.compareTo(remoteCounter)> 0){
 		        		break;
 		        	}            			 
            		 continue;
    			}else{
    				processedFiles++;
    			}
            	// change sets exists, reset missing URLs
                 missing_urls = 0;
                 
                 System.out.println("Processed files = "+processedFiles);
                 
                 Model srcAdded = ModelFactory.createDefaultModel();
                 Model srcRemoved = ModelFactory.createDefaultModel();
                 
                 Model targetAdded = ModelFactory.createDefaultModel();
                 Model targetRemoved = ModelFactory.createDefaultModel();
                 
                 if(srcAddedchanges.exists())
                	 srcAdded = srcAdded.read(srcAddedTriplesURL);
                 
                 if(srcDeletedchanges.exists())
                	 srcRemoved.read(srcDeletedTriplesURL);
                 
                 if(targetAddedchanges.exists())
                	 targetAdded.read(targetAddedTriplesURL);
                 
                 if(targetDeletedchanges.exists())
                	 targetRemoved.read(targetDeletedTriplesURL);
               
                 
                 SyncManager manager = new SyncManager(srcAdded, srcRemoved, targetAdded, targetRemoved, SynchronizationStrategy.STRATEGY_I);
                 
                 System.out.println("Starting synchronization ....");
                 List<Model> result = manager.sync();
                 System.out.println(result.get(0).size() + " triples added and " + result.get(1).size() + " triples removed" );
                 
                 // save last processed date
                 LastDownloadDateManager.writeLastDownloadDate(LAST_DOWNLOAD, currentCounter.toString());
               //  logger.info("Incrementing changeseet counter .. .");
                 //increment to next counter
                 currentCounter.incrementCount();
	        }
		}
	}

}

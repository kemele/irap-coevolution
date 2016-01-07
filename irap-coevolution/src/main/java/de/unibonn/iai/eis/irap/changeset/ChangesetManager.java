/**
 * 
 */
package de.unibonn.iai.eis.irap.changeset;

import java.io.File;


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
                 
                /* Model addedTriples = ModelFactory.createDefaultModel();
                 Model removedTriples = ModelFactory.createDefaultModel();
                 
                 if(addedchanges.exists()){
                	 addedTriples = RDFDataMgr.loadModel(addedTriplesURL);
                 }
                 
                 if(deletedchanges.exists()){
                	 removedTriples = RDFDataMgr.loadModel(deletedTriplesURL);
                 }*/
                 System.out.print(processedFiles);
                 if(srcAddedchanges.exists())
                	 System.out.print(srcAddedTriplesURL + " ==== ");
                 
                 if(srcDeletedchanges.exists())
                	 System.out.print(srcDeletedTriplesURL + " ==== ");
                 
                 if(targetAddedchanges.exists())
                	 System.out.print(targetAddedTriplesURL + " ==== ");
                 
                 if(targetDeletedchanges.exists())
                	 System.out.print(targetDeletedTriplesURL + " ==== ");
                 System.out.println();
                 //Changeset changeset = new Changeset(folder, removedTriples, addedTriples, currentCounter.getSequenceNumber());	        
                 
                 //Notify evaluator
               //  logger.info("Notifying interest evaluation manager by sending changeset triples .....");
                 
                 //InterestEvaluationManager eval= new InterestEvaluationManager(interestManager, changeset);
                 //eval.begin();
                 
                 
                // logger.info("Updating last processed changeset data ...");
                 // save last processed date
                 LastDownloadDateManager.writeLastDownloadDate(LAST_DOWNLOAD, currentCounter.toString());
               //  logger.info("Incrementing changeseet counter .. .");
                 //increment to next counter
                 currentCounter.incrementCount();
	        }
		}
	}

}

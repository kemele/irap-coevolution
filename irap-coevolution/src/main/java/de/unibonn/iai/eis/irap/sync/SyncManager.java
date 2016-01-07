/**
 * 
 */
package de.unibonn.iai.eis.irap.sync;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.tdb.TDBFactory;

/**
 * @author Kemele M. Endris
 *
 */
public class SyncManager {

	
	private Dataset sourceDS  = TDBFactory.createDataset("srcDs");
	private Dataset targetDS = TDBFactory.createDataset("targetDs");
	
	public Model sourceAdded = ModelFactory.createDefaultModel();
	public Model sourceRemoved = ModelFactory.createDefaultModel();
	
	public Model targetAdded = ModelFactory.createDefaultModel();
	public Model targetRemoved = ModelFactory.createDefaultModel();
	
	private int syncStrategy = 1;
	
	public SyncManager(Model sourceAdded, Model sourceRemoved, Model targetAdded, Model targetRemoved, int strategy) {
		this.sourceAdded = sourceAdded;
		this.sourceRemoved = sourceRemoved;
		this.targetAdded = targetAdded;
		this.targetRemoved = targetRemoved;
		this.syncStrategy = strategy;
	}
	
	public List<Model> sync(){
		
		List<Model> result = new ArrayList<Model>();
		
		Model r  = processRemoved();		
		applyRemoved(r);
		
		Model a = processAdded();
		applyAdded(a);
		
		result.add(a);
		result.add(r);
		return result;
	}
	
	
	/**
	 * No Conflict checking for removed triples
	 * 
	 * removes sourceRemoved triples from targetDS
	 * and targetRemoved triples form sourceDS  
	 */
	private Model processRemoved(){
		Model result = targetRemoved.union(sourceRemoved);
		
			
		return result;
	}
	
	private Model processAdded(){
		switch(syncStrategy){
			case SynchronizationStrategy.STRATEGY_I:
				return preferSource();
			case SynchronizationStrategy.STRATEGY_II:
				return preferTarget();
			case SynchronizationStrategy.STRATEGY_III:
				return ignoreConflicts();
			case SynchronizationStrategy.STRATEGY_IV:
				return resolveConflicts();
			default:
				return null;
		}
		
		
	}
	/**
	 * STRATEGY I:
	 *   add non-conflicting triples to datasets and then
	 *   ignore conflicting triples from target but 
	 *   apply only conflicting triples from source
	 * @return a model that contains all triples that should be added in both datasets
	 */
	private Model preferSource(){
		List<Model> potentialConflits = getPotentialConflicts();
		
		Model nonSource = sourceAdded.difference(potentialConflits.get(0));
		Model nonTarget = targetAdded.difference(potentialConflits.get(1));
		//union of non conflicting source and target triples added and conflicting triples from source (preferred triples)
		 return nonSource.union(nonTarget).union(potentialConflits.get(0));
	}
	/**
	 * STRATEGY II:
	 *   add non-conflicting triples to datasets and then
	 *   ignore conflicting triples from source but 
	 *   apply only conflicting triples from target
	 * @return a model that contains all triples that should be added in both datasets
	 */
	private Model preferTarget(){
		List<Model> potentialConflits = getPotentialConflicts();
		
		Model nonSource = sourceAdded.difference(potentialConflits.get(0));
		Model nonTarget = targetAdded.difference(potentialConflits.get(1));
		//union of non conflicting source and target triples added and conflicting triples from target (preferred triples)
		 return nonSource.union(nonTarget).union(potentialConflits.get(1));
	}
	/**
	 * STRATEGY III:
	 *   add non-conflicting triples to datasets and then
	 *   ignore conflicting triples from both dataset but 
	 *   add removed triples back to each dataset (those that matches subject and predicates in potentially Conflicting triples)
	 * @return a model that contains all triples that should be added in both datasets
	 */
	private Model ignoreConflicts(){
		List<Model> potentialConflits = getPotentialConflicts();
		
		Model nonSource = sourceAdded.difference(potentialConflits.get(0));
		Model nonTarget = targetAdded.difference(potentialConflits.get(1));
		
		//get triples from removed that matches same subject and predicate (this probably was rename(value change) operation
		List<Model>  remed = getRelatedFromRemoved(potentialConflits);
		
		//union of non conflicting source and target triples added and triples matching conflicting triples from removed part of each dataset
		 return nonSource.union(nonTarget).union(remed.get(0)).union(remed.get(1));
	}
	/**
	 * STRATEGY IV:
	 *   add non-conflicting triples to datasets and then
	 *   resolve conflicting triples from both dataset and add them to both datasets.
	 *   
	 * @return a model that contains all triples that should be added to both datasets
	 */
	private Model resolveConflicts(){
		List<Model> potentialConflits = getPotentialConflicts();
		
		Model nonSource = sourceAdded.difference(potentialConflits.get(0));
		Model nonTarget = targetAdded.difference(potentialConflits.get(1));
		
		Model resolved = resolve(potentialConflits);
		//union of non conflicting source and target triples added and conflicting triples from target (preferred triples)
		 return nonSource.union(nonTarget).union(resolved);
	}

	//TODO: check resolution functions 
	/**
	 * resolve conflicts based on resolution functions setting
	 * 
	 * @param potentialConflits
	 * @return
	 */
	private Model resolve(List<Model> potentialConflits){
		Model result = ModelFactory.createDefaultModel();
		
		return result;
	}
	/**
	 * get triples from removed that matches same subject and predicate (this probably was rename(value change) operation
	 * 
	 * @param potentialConflicts
	 * @return
	 */
	private List<Model> getRelatedFromRemoved(List<Model> potentialConflicts){
		Model sadd = ModelFactory.createDefaultModel();
		Model tadd = ModelFactory.createDefaultModel();
		
		StmtIterator siter = potentialConflicts.get(0).listStatements();
		while(siter.hasNext()){
			Statement st = siter.next();
			if(sourceRemoved.contains(st.getSubject(), st.getPredicate())){				
				NodeIterator niter = sourceRemoved.listObjectsOfProperty(st.getSubject(), st.getPredicate());
				while(niter.hasNext()){
					RDFNode o = niter.next();					
					sadd.add(st.getSubject(), st.getPredicate(), o);
				}
				
			}
		}
		StmtIterator titer = potentialConflicts.get(1).listStatements();
		while(titer.hasNext()){
			Statement st = titer.next();
			if(targetRemoved.contains(st.getSubject(), st.getPredicate())){				
				NodeIterator niter = targetRemoved.listObjectsOfProperty(st.getSubject(), st.getPredicate());
				while(niter.hasNext()){
					RDFNode o = niter.next();					
					tadd.add(st.getSubject(), st.getPredicate(), o);
				}
				
			}
		}
		
		List<Model> result = new ArrayList<Model>();
		result.add(sadd);
		result.add(tadd);
		return result;
	}
	/**
	 * finds potentially conflicting triples from sourceAdded and targetAdded models
	 * By (our) definition: potentially conflicting triples are triples with same subject and predicate but different object value
	 *  
	 * @return  List of models: at 
	 *      0 = conflicting from source and 
	 *      1 = potentially conflicting from target
	 */
	private List<Model> getPotentialConflicts(){
		Model sadd = ModelFactory.createDefaultModel();
		Model tadd = ModelFactory.createDefaultModel();
		
		StmtIterator siter = sourceAdded.listStatements();
		while(siter.hasNext()){
			Statement st = siter.next();
			if( targetAdded.contains(st.getSubject(), st.getPredicate())){
				sadd.add(st);
				NodeIterator niter = targetAdded.listObjectsOfProperty(st.getSubject(), st.getPredicate());
				while(niter.hasNext()){
					RDFNode o = niter.next();					
					tadd.add(st.getSubject(), st.getPredicate(), o);
				}
				
			}
		}
		List<Model> result = new ArrayList<Model>();
		result.add(sadd);
		result.add(tadd);
		return result;
				
	}
	
	
	private void applyRemoved(Model r){
		sourceDS.getDefaultModel().remove(r);
		targetDS.getDefaultModel().remove(r);
	}
	private void applyAdded(Model a){
		sourceDS.getDefaultModel().add(a);
		targetDS.getDefaultModel().add(a);
	}
	
}

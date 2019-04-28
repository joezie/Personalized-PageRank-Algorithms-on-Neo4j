package joezie.fora_neo4j;

public interface Preprocessing_Interface {
	public void preprocessing(Double threshold, Object param); 
	// param is the parameter used to adjust accuracy of Whole-Graph SSPPR algo; 
	// in the end, we only store ppr values that are not smaller than threshold
	
	public void readPreprocessedPPR(Long nodeId_start);
	// read pprs from file corresponding to src node and store in a member variable 
	
	//public Object getPreprocessedPPR();
	// return the reference of the member variable that stores the preprocessed ppr results
	
	public Long getPrepSize();
	// get the size of the preprocessing directory in bytes
	
	public void deletePrepDir();
	// delete the preprocessing directory
}

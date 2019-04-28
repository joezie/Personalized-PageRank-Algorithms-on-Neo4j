package joezie.fora_neo4j;

import java.util.HashMap;

public interface Whole_Graph_Util_Interface {
	public void printWholeGraphResult(); // print ppr result and other related results if provided
	
	public HashMap<Long, Double> getWholeGraphPPR(); // get the reference of the ppr result

	public void computeWholeGraphPPR(Long nodeId_start, Object param); 
	// compute estimated ppr; param is the parameter used to adjust accuracy of PPR algo;
}

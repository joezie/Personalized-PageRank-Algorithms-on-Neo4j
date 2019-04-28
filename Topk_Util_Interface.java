package joezie.fora_neo4j;

import java.util.Vector;

public interface Topk_Util_Interface {
	public void printTopKResult(int k); 
	// print ppr result and other related results if provided
	
	public void computeTopKPPR(Long nodeId_start, int k, Object param); 
	// compute top-k SSPPR;
	// param is the parameter used to adjust accuracy of PPR algo;
	
	public Vector<Long> getTopKNodeIds(int k); 
	// return the top-k node ids sorted by ppr
}

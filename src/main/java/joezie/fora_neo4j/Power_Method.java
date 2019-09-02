package joezie.fora_neo4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class Power_Method extends Algo_Util implements Whole_Graph_Util_Interface, Topk_Util_Interface { // computing ground truth of ppr
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph
	private HashMap<Long, Double> residue; // propagated value, i.e r
	private HashMap<Long, Double> reserve; // stored value, i.e pi
	private HashMap<Long, Double> topk_res; // top-k ppr result
	private Vector<Long> topk_nodeIds; // top-k node ids sorted by ppr
	private Double alpha;
		
	public Power_Method(GraphDatabaseService graphDb, Double alpha, Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		topk_res = new HashMap<>();
		topk_nodeIds = new Vector<>();
		this.alpha = alpha;
	}
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object dummy) { 
		// compute exact ppr by performing forward push for 100 iterations
		
		// 1. clear
		reserve.clear();
		residue.clear();
		topk_res.clear();
		topk_nodeIds.clear();
		
		// 2. perform forward push for 100 times
		residue.put(nodeId_start, 1.0); // r(s,s) = 1.0
		int iterations = 100;
		
		for (int num_iter = 0; num_iter < iterations; num_iter++) {
			HashMap<Long, Double> pairs = new HashMap<>(residue);
			residue.clear();
			for (Map.Entry<Long, Double> entry : pairs.entrySet()) {
				Long nodeId_cur = entry.getKey();
				Double residue_cur = entry.getValue();
				int nodeIdM_cur = adjM.toMappedNodeId(nodeId_cur); // current node id in adjacency matrix
				int out_degree_cur = adjM.degree(nodeIdM_cur, Direction.OUTGOING);
				
				if (entry.getValue() > 0) {
					Double old_reserve_cur = reserve.get(nodeId_cur);
					if (old_reserve_cur == null)
						old_reserve_cur = 0.0;
					reserve.put(nodeId_cur, old_reserve_cur + residue_cur * alpha);
					// pi(s,v) = pi(s,v) + alpha * r(s,v)
					
					Double residue_remain = residue_cur * (1 - alpha);
					if (out_degree_cur == 0) {
						// no out neighbors, then pass the remaining residue to start
						Double old_residue_start = residue.get(nodeId_start);
						if (old_residue_start == null)
							old_residue_start = 0.0;
						residue.put(nodeId_start, old_residue_start + residue_remain);
					}
					else {
						Double avg_push_residue = residue_remain / out_degree_cur;
												
						adjM.forEachRelationship(nodeIdM_cur, Direction.OUTGOING, 
								(nodeIdM1, nodeIdM2, tmp) -> {
									Long nodeId1 = adjM.toOriginalNodeId(nodeIdM1);
									Long nodeId2 = adjM.toOriginalNodeId(nodeIdM2);
									Double old_residue_next = residue.get(nodeId2);
									if (old_residue_next == null)
										old_residue_next = 0.0;
									Double new_residue_next = old_residue_next + avg_push_residue;
									residue.put(nodeId2, new_residue_next);
									//r(s,u) = r(s,u) + (1 - alpha) * r(s,v) / |N_out(v)|
										
									return true;
								});
					}
				}
			}
		}
	}
	
	@Override
	public void printWholeGraphResult() {
		List<Map.Entry<Long, Double>> reserve_list = new ArrayList<Map.Entry<Long, Double>>(reserve.entrySet());
		reserve_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		
		System.out.println("Exact-PPR:");
		for (Map.Entry<Long, Double> reserve_t : reserve_list) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue());
	}

	@Override
	public HashMap<Long, Double> getWholeGraphPPR() { // return the reference of reserve
		return reserve;
	}
	
	public HashMap<Long, Double> getTopK(int k) { // return the reference of topk_res; might include more than k nodes
		return topk_res;
	}
	
	@Override
	public Vector<Long> getTopKNodeIds(int k) { // return the reference of topk_nodeIds sorted by ppr; might include more than k nodes
		if (!topk_nodeIds.isEmpty()) // topk_node_ids is already set, then no need to compute again 
			return topk_nodeIds;		
		
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(topk_res.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) // store node ids in topk_node_ids after sorting
			topk_nodeIds.add(topk_res_list_t.getKey());
		return topk_nodeIds;
	}
	
	@Override
	public void computeTopKPPR(Long nodeId_start, int k, Object dummy) { 
		// store the top-k results in topk_res; might include more than k nodes
		
		// 1. perform Power Method Whole-Graph SSPPR algo
		computeWholeGraphPPR(nodeId_start, dummy);
		
		// 2. retrieve top-k results and store in topk_res 
		Double kth_reserve = kth_ppr(reserve.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			topk_res = new HashMap<>(reserve);
			return;
		}
			
		for (Map.Entry<Long, Double> reserve_t : reserve.entrySet()) {
			// find the pprs not smaller than the kth ppr, so it's possible that
			// the size of topk_res is greater than k
			if (reserve_t.getValue() >= kth_reserve)
				topk_res.put(reserve_t.getKey(), reserve_t.getValue());
		}
		return;
	}
	
	@Override
	public void printTopKResult(int k) {
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(topk_res.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		int count = 0;
		System.out.println("\nExact-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
		}
	}
}

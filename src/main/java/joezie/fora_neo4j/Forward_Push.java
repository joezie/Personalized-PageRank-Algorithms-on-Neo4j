package joezie.fora_neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.AbstractQueue;
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

import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.internal.compiler.v2_3.spi.TheCookieManager;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;


public class Forward_Push extends Algo_Util implements Whole_Graph_Util_Interface, Preprocessing_Interface, Topk_Util_Interface { // Forward Push algorithm
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph
	private HashMap<Long, Double> residue; // (node id in graphDb, propagated value, i.e r)
	private HashMap<Long, Double> reserve; // (node id in graphDb, stored value, i.e pi)
	Queue<Long> Q_next; // nodes that might still propagate forward in the next call 
						// meet the requirement of r(s,v) / |N_out(v)| > min_rmax	
	private int node_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	private Double rsum; // the sum of all nodes' residues(r) during a local update process from s
	private String preprocessing_dirName;
	private Vector<Long> topk_nodeIds; // top-k node ids sorted by ppr
	private HashMap<Long, Double> topk_res; // top-k ppr result
	
	public Forward_Push(Double alpha, Double rsum, int node_amount, GraphDatabaseService graphDb,
			Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		Q_next = new ConcurrentLinkedQueue<>();
		topk_nodeIds = new Vector<>();
		topk_res = new HashMap<>();
		this.node_amount = node_amount;
		this.alpha = alpha;
		this.rsum = rsum;
		preprocessing_dirName = "FWP_ppr_results/" + dir_db;
	}
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object rmax) { // forward push for whole graph ppr
		residue.clear();
		reserve.clear();
		topk_nodeIds.clear();
		topk_res.clear();
		Double rsum_local = 1.0;
		int nodeIdM_start = adjM.toMappedNodeId(nodeId_start); // start node id in adjacency matrix
		int out_degree_start = adjM.degree(nodeIdM_start, Direction.OUTGOING);
		
		if (out_degree_start == 0) { // terminate if start node's out degree is 0
			reserve.put(nodeId_start, 1.0);
			rsum = 0.0;
			return;
		}
		
		HashSet<Long> nodesInQueue = new HashSet<>(); // record nodes in Q
		Queue<Long> Q = new ConcurrentLinkedQueue<>(); // nodes that can still propagate forward
				
		Q.offer(nodeId_start);
		nodesInQueue.add(nodeId_start);
		residue.put(nodeId_start, 1.0); // r(s,s) = 1.0
		
		while (!Q.isEmpty()) {
			Long nodeId_cur = Q.poll();
			nodesInQueue.remove(nodeId_cur);
			Double residue_cur = residue.get(nodeId_cur);
			residue.replace(nodeId_cur, 0.0); // r(s,v)=0
			
			Double old_reserve_cur = reserve.get(nodeId_cur);
			if (old_reserve_cur == null)
				old_reserve_cur = 0.0;
			reserve.put(nodeId_cur, old_reserve_cur + residue_cur * alpha);
			// pi(s,v) = pi(s,v) + alpha * r(s,v)
			
			rsum_local -= residue_cur * alpha; //update rsum_local
			int nodeIdM_cur = adjM.toMappedNodeId(nodeId_cur); // current node id in adjacency matrix
			int out_degree_cur = adjM.degree(nodeIdM_cur, Direction.OUTGOING);
			
			if (out_degree_cur == 0) {
				// no out neighbor, then consider start node as its only neighbor
				// and propagate (1 - alpha) * r(s,v) to start node
				Double new_residue_start = residue.get(nodeId_start) + residue_cur * (1.0 - alpha);
				residue.put(nodeId_start, new_residue_start);
				
				if (out_degree_start > 0 && new_residue_start / (double)out_degree_start >= (Double)rmax 
						&& !nodesInQueue.contains(nodeId_start)) {
					// check whether the updated start node should be added into Q or not
					// condition: meet the requirement of Q and not in Q currently
					Q.offer(nodeId_start);
					nodesInQueue.add(nodeId_start);
				}
				continue;
			}
			
			Double avg_push_residue = ((1.0 - alpha) * residue_cur) / (double)out_degree_cur;
						
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
									
					int out_degree_next = adjM.degree(nodeIdM2, Direction.OUTGOING);
					
					if (new_residue_next / (double)out_degree_next >= (Double)rmax && !nodesInQueue.contains(nodeId2)) {
						// check whether the updated next node should be added into Q or not
						// condition: meet the requirement of Q and not in Q currently
						Q.offer(nodeId2);
						nodesInQueue.add(nodeId2);
					}
					return true;
				});
			rsum = rsum_local; // update rsum
		}
	}
	
	public void forward_push_topk(Long nodeId_start, Queue<Long> Q, Double min_rmax, boolean isFirstFwdpush, 
			Double rmax) { // forward push for topk ppr
		int nodeIdM_start = adjM.toMappedNodeId(nodeId_start); // start node id in adjacency matrix
		int out_degree_start = adjM.degree(nodeIdM_start, Direction.OUTGOING);
		
		if (out_degree_start == 0) { // terminate if start node's out degree is 0
			reserve.put(nodeId_start, 1.0);
			rsum = 0.0;
			return;
		}
		
		if (isFirstFwdpush) // initialize residue if it's the first forward push
			residue.put(nodeId_start, 1.0);
		Q_next.clear(); // clear Q_next to be ready for storing new ones
		Double rsum_local = rsum;
		
		HashSet<Long> nodesInQueue = new HashSet<>(); // record nodes in Q
		HashSet<Long> nodesInQueue_next = new HashSet<>(); // record nodes in Q_next
		
		nodesInQueue.addAll(Q); // initialize nodesInQueue
		
		while (!Q.isEmpty()) {
			Long nodeId_cur = Q.poll();
			nodesInQueue.remove(nodeId_cur);
			Double residue_cur = residue.get(nodeId_cur);
			
			int nodeIdM_cur = adjM.toMappedNodeId(nodeId_cur); // current node id in adjacency matrix
			int out_degree_cur = adjM.degree(nodeIdM_cur, Direction.OUTGOING);
			
			if (residue_cur / out_degree_cur >= rmax) {
				// since it's not guaranteed that the nodes in Q meet the
				// requirement that r(s,v)/|N_out(v)|>r_max, we need to check
				residue.replace(nodeId_cur, 0.0); // r(s,v)=0
			
				Double old_reserve_cur = reserve.get(nodeId_cur);
				if (old_reserve_cur == null)
					old_reserve_cur = 0.0;
				reserve.put(nodeId_cur, old_reserve_cur + residue_cur * alpha);
				// pi(s,v) = pi(s,v) + alpha * r(s,v)
			
				rsum_local -= residue_cur * alpha; //update rsum_local
			
				if (out_degree_cur == 0) {
					// no out neighbor, then consider start node as its only neighbor
					// and propagate (1 - alpha) * r(s,v) to start node
					Double new_residue_start = residue.get(nodeId_start) + residue_cur * (1 - alpha);
					residue.put(nodeId_start, new_residue_start);
				
					
					if (out_degree_start > 0 && new_residue_start / (double)out_degree_start >= rmax 
							&& !nodesInQueue.contains(nodeId_start)) {
						// check whether the updated start node should be added into Q or not
						// condition: meet the requirement of Q and not in Q currently
						Q.offer(nodeId_start);
						nodesInQueue.add(nodeId_start);
					}
					else if (out_degree_start > 0 && new_residue_start / (double)out_degree_start >= min_rmax 
							&& !nodesInQueue_next.contains(nodeId_start)) {
						// check whether the updated start node should be added into Q_next or not
						// condition: meet the requirement of Q_next and not in Q_next currently
						 
						nodesInQueue_next.add(nodeId_start);
						Q_next.offer(nodeId_start);
					}
					continue;
				}
			
				Double avg_push_residue = ((1.0 - alpha) * residue_cur) / (double)out_degree_cur;
								
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
											
							int out_degree_next = adjM.degree(nodeIdM2, Direction.OUTGOING);
							
							if (new_residue_next / (double)out_degree_next >= rmax && !nodesInQueue.contains(nodeId2)) {
								// check whether the updated next node should be added into Q or not
								// condition: meet the requirement of Q and not in Q currently
								Q.offer(nodeId2);
								nodesInQueue.add(nodeId2);
							}
							else if (new_residue_next / (double)out_degree_next >= min_rmax && !nodesInQueue_next.contains(nodeId2)) {
								// check whether the updated next node should be added into Q_next or not
								// condition: meet the requirement of Q_next and not in Q_next currently
								nodesInQueue_next.add(nodeId2);
								Q_next.offer(nodeId2);
							}
							return true;
						});
			}
			else if (residue_cur / (double)out_degree_cur >= min_rmax && !nodesInQueue_next.contains(nodeId_cur)) {
				// check whether the current node should be added into Q_next or not
				// condition: meet the requirement of Q_next and not in Q_next currently
				 
				nodesInQueue_next.add(nodeId_cur);
				Q_next.offer(nodeId_cur);
			}
		}
		rsum = rsum_local; // update rsum
	}
		
	public Double getUpdatedRsum() {
		return rsum;
	}
	
	public HashMap<Long, Double> getResidueCopy() { // copy the hashmap residue and return
		return new HashMap<>(residue);
	}

	public HashMap<Long, Double> getReserveCopy() { // copy the hashmap reserve and return
		return new HashMap<>(reserve);
	}
	
	public Queue<Long> getNextQueueCopy() { // copy the queue Q_next and return
		return new ConcurrentLinkedQueue<>(Q_next); 
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
		
		System.out.println("Forward-Push Reserve(pi) & Residue(r):");
		for (Map.Entry<Long, Double> reserve_t : reserve_list) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue() + '\t' + residue.get(reserve_t.getKey()));
	}

	@Override
	public HashMap<Long, Double> getWholeGraphPPR() { // return the reference of reserve
		return reserve;
	}
	
	@Override
	public void preprocessing(Double dummy, Object rmax) {
		// customize preprocessing_dirName
		preprocessing_dirName += ("/" + rmax);
		
		System.out.println("\nForward Push preprocessing started...");
		HashMap<Integer, Integer> prog_pct_map = new HashMap<>(); // (nodeId, progress percentage)
		for (int i = 10; i < 100; i += 10) {
			int nodeId_i = node_amount * i / 100;
			prog_pct_map.put(nodeId_i, i);
		}
		prog_pct_map.put(node_amount - 1, 100);
		
		File dirPath = new File(preprocessing_dirName);
		if (!dirPath.exists()) { // create directory if not exists
			dirPath.mkdirs();
			System.out.println("\nCreated directory " + preprocessing_dirName);
		}
		else { // if exists, clear it before preprocessing
			try {
				FileUtils.cleanDirectory(dirPath);
			}
			catch (IOException e) {
				System.out.println("Clear directory " + preprocessing_dirName + " failed!");
				e.printStackTrace();
			}
			System.out.println("\nDirectory " + preprocessing_dirName + " cleared");
		}
		
		adjM.forEachNode(nodeIdM -> {
			Long nodeId_start = adjM.toOriginalNodeId(nodeIdM);
			computeWholeGraphPPR(nodeId_start, rmax); // run Forward Push on src node
			
			String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
			try { // store ppr values that are not smaller than threshold
				FileWriter fw = new FileWriter(fileName_start, false);
				for (Map.Entry< Long, Double > entry_t : reserve.entrySet()) {
					Long nodeId_i = entry_t.getKey();
					Double ppr_i = entry_t.getValue();
					fw.write(nodeId_i.toString() + '\t' + ppr_i.toString() + '\n');
				}
				fw.close();
			}
			catch (IOException e) {
				System.out.println("Write to file " + fileName_start + " failed!");
				e.printStackTrace();
			}
			
			if (prog_pct_map.containsKey(nodeIdM))
				System.out.println("Progress: " + prog_pct_map.get(nodeIdM) + "%");
			return true;
		});
	}

	@Override
	public void readPreprocessedPPR(Long nodeId_start) {
		// read pprs from file corresponding to src node and store in ppr
		reserve.clear();
		String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(fileName_start)));
			String tmpStr = null;
			while ((tmpStr = br.readLine()) != null) {
				int space_idx = tmpStr.indexOf('\t');
				Long nodeId_target = Long.parseLong(tmpStr.substring(0, space_idx));
				Double ppr_target = Double.parseDouble(tmpStr.substring(space_idx + 1));
				reserve.put(nodeId_target, ppr_target);
			}
			br.close();
		}
		catch (IOException e) {
			System.out.println("Read from file " + fileName_start + " failed!");
			e.printStackTrace();
		}
	}
	
	@Override
	public void computeTopKPPR(Long nodeId_start, int k, Object rmax) { 
		// store the top-k results in topk_res; might include more than k nodes
		
		computeWholeGraphPPR(nodeId_start, rmax);
		// perform Forward Push Whole-Graph SSPPR algo
	}
	
	@Override
	public void printTopKResult(int k) {
		if (topk_res.isEmpty())
			retrieveTopK(k);
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(topk_res.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		int count = 0;
		System.out.println("\nForward-Push-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
		}
	}
	
	@Override
	public Vector<Long> getTopKNodeIds(int k) { // return the reference of topk_nodeIds sorted by ppr; might include more than k nodes
		if (!topk_nodeIds.isEmpty()) // topk_node_ids is already set, then no need to compute again 
			return topk_nodeIds;	
		
		if (topk_res.isEmpty())
			retrieveTopK(k);
		
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
	
	private void retrieveTopK(int k) { // retrieve top-k results from whole-graph ppr results and store in topk_res
		if (!topk_res.isEmpty())
			return;
		
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
	}

	@Override
	public Long getPrepSize() {
		return FileUtils.sizeOfDirectory(new File(preprocessing_dirName));
	}

	@Override
	public void deletePrepDir() {
		try {
			FileUtils.deleteDirectory(new File(preprocessing_dirName));
		} catch (IOException e) {
			System.out.println("Clear directory " + preprocessing_dirName + " failed!");
			e.printStackTrace();
		}
	}
}

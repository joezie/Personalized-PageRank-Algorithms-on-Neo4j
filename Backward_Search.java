package joezie.fora_neo4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.security.AccessMode.Static;

public class Backward_Search { // All-Pair-Backward-Search algorithm based on backward searches
	private GraphDatabaseService graphDb;
	Graph adjM; // adjacency matrix of the graph
	private HashMap<Long, Double> residue; // propagated value, i.e r
	private HashMap<Long, Double> reserve; // stored value, i.e pi
	private int node_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	private Double rmax; // the residue(r) threshold for local update
	
	public Backward_Search(Double alpha, Double rmax, int node_amount, GraphDatabaseService graphDb,
			Graph adjM) {
		this.graphDb = graphDb;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		this.node_amount = node_amount;
		this.alpha = alpha;
		this.rmax = rmax;
	}
	
	private static long startTime = 0, endTime = 0, duration = 0;
	
	public void backward_search_whole_graph(Long nodeId_target) { // Single-Source PPR compute
		startTime = System.nanoTime();
		residue.clear();
		reserve.clear();
		
		int nodeIdM_target = adjM.toMappedNodeId(nodeId_target); // target node id in adjacency matrix
		int in_degree_target = adjM.degree(nodeIdM_target, Direction.INCOMING);
		
		if (in_degree_target == 0) { // terminate if target node's in-degree is 0
			reserve.put(nodeId_target, 1.0);
			return;
		}
		
		HashSet<Long> nodesInQueue = new HashSet<>(); // record nodes in Q
		Queue<Long> Q = new ConcurrentLinkedQueue<>(); // nodes that can still propagate backward
				
		Q.offer(nodeId_target);
		nodesInQueue.add(nodeId_target);
		residue.put(nodeId_target, 1.0); // r(t,t) = 1.0
		
		while (!Q.isEmpty()) {
			Long nodeId_cur = Q.poll();
			nodesInQueue.remove(nodeId_cur);
			Double residue_cur = residue.get(nodeId_cur);
			residue.replace(nodeId_cur, 0.0); // r(v,t)=0
			
			Double old_reserve_cur = reserve.get(nodeId_cur);
			if (old_reserve_cur == null)
				old_reserve_cur = 0.0;
			reserve.put(nodeId_cur, old_reserve_cur + residue_cur * alpha);
			// pi(v, t) = pi(v, t) + alpha * r(v, t)
							
			int nodeIdM_cur = adjM.toMappedNodeId(nodeId_cur); // current node id in adjacency matrix
			int in_degree_cur = adjM.degree(nodeIdM_cur, Direction.INCOMING);
			
			Double avg_push_residue = ((1.0 - alpha) * residue_cur);
			// Note: not divided by in_degree(next_node) yet
		
			
			adjM.forEachRelationship(nodeIdM_cur, Direction.INCOMING, 
					(nodeIdM1, nodeIdM2, tmp) -> {
						Long nodeId1 = adjM.toOriginalNodeId(nodeIdM1);
						Long nodeId2 = adjM.toOriginalNodeId(nodeIdM2);
						Double old_residue_next = residue.get(nodeId2);
						if (old_residue_next == null)
							old_residue_next = 0.0;
						int out_degree_next = adjM.degree(nodeIdM2, Direction.OUTGOING);
						Double new_residue_next = old_residue_next + avg_push_residue / out_degree_next;
						residue.put(nodeId2, new_residue_next);
						//r(u, t) = r(u, t) + (1 - alpha) * r(v, t) / |N_out(u)|
										
						if (new_residue_next > rmax && !nodesInQueue.contains(nodeId2)) {
							// check whether the updated next node should be added into Q or not
							// condition: meet the requirement of Q and not in Q currently
							Q.offer(nodeId2);
							nodesInQueue.add(nodeId2);
						}
						return true;
					});
		}
		endTime = System.nanoTime();
		duration += (endTime - startTime);
	}

	public HashMap<Long, Double> getReserve() { // return reference of hashmap reserve
		return reserve;
	}
	
	public long getDuration() {
		return duration;
	}
}

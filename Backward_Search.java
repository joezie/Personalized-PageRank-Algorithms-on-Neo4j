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

public class Backward_Search { // Backward Search Preprocessing algorithm (BASE) based on backward searches
	private GraphDatabaseService graphDb;
	Graph adjM; // adjacency matrix of the graph
	private HashMap<Long, Double> residue; // propagated value, i.e r
	private HashMap<Long, Double> reserve; // stored value, i.e pi
	//Queue<Node> Q_next; // nodes that might still propagate forward in the next call 
						// meet the requirement of r(s,v) / |N_out(v)| > min_rmax
	//HashMap<Node, Integer> in_degree_map; // record the in-going degree for each node
	//HashMap<Node, Iterable<Relationship>> rel_iter_map; // record the in-going relationship iterable for each node
	
	// tag::configuration parameters[]
	private int node_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	private Double rmax; // the residue(r) threshold for local update
	// end::configuration parameters[]
	
	public Backward_Search(Double alpha, Double rmax, int node_amount, GraphDatabaseService graphDb,
			Graph adjM) {
		this.graphDb = graphDb;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		//Q_next = new ConcurrentLinkedQueue<>();
		//this.in_degree_map = in_degree_map;
		//this.rel_iter_map = rel_iter_map;
		
		// tag::configuration parameters assignment[]
		this.node_amount = node_amount;
		this.alpha = alpha;
		this.rmax = rmax;
		// end::configuration parameters assignment[]
	}
	
	private static long startTime = 0, endTime = 0, duration = 0;
	
	public void backward_search_whole_graph(Long nodeId_target) { // backward push for whole graph ppr
		startTime = System.nanoTime();
		residue.clear();
		reserve.clear();
		
		// tag::new version[]
		//int in_neighbor_target = in_degree_map.get(target);
		int nodeIdM_target = adjM.toMappedNodeId(nodeId_target); // target node id in adjacency matrix
		int in_degree_target = adjM.degree(nodeIdM_target, Direction.INCOMING);
		
		if (in_degree_target == 0) { // terminate if target node's in-degree is 0
			reserve.put(nodeId_target, 1.0);
			return;
		}
		// end::new version[]
		
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
							
	        //int in_neighbor_cur = in_degree_map.get(cur_node);
			int nodeIdM_cur = adjM.toMappedNodeId(nodeId_cur); // current node id in adjacency matrix
			int in_degree_cur = adjM.degree(nodeIdM_cur, Direction.INCOMING);
						
			/*
			if (in_degree_cur == 0) {
				// no in neighbor, then consider target node as its only neighbor
				// and propagate (1 - alpha) * r(v, t) to target node
				Double new_residue_target = residue.get(nodeId_target) + residue_cur * (1.0 - alpha);
				residue.put(nodeId_target, new_residue_target);
				
				if (new_residue_target > rmax && !nodesInQueue.contains(nodeId_target)) {
					// check whether the updated target node should be added into Q or not
					// condition: meet the requirement of Q and not in Q currently
					Q.offer(nodeId_target);
					nodesInQueue.add(nodeId_target);
				}
				continue;
			}
			*/
			
			Double avg_push_residue = ((1.0 - alpha) * residue_cur);// Note: not divided by in_degree(next_node) yet
			
			/*
			Iterator<Relationship> iter = rel_iter_map.get(cur_node).iterator();
			
			while (iter.hasNext()) {
				Node next_node = iter.next().getOtherNode(cur_node);
		        
				Double old_residue_next = residue.get(next_node);
				if (old_residue_next == null)
					old_residue_next = 0.0;
				Double new_residue_next = old_residue_next + avg_push_residue;
				residue.put(next_node, new_residue_next);
				//r(s,u) = r(s,u) + (1 - alpha) * r(s,v) / |N_out(v)|
								
				int in_neighbor_next = in_degree_map.get(next_node);
				
				if (new_residue_next / (double)in_neighbor_next >= rmax && !nodesInQueue.contains(next_node)) {
					// check whether the updated next node should be added into Q or not
					// condition: meet the requirement of Q and not in Q currently
					Q.offer(next_node);
					nodesInQueue.add(next_node);
				}
			}
			*/
			
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
										
						//int out_neighbor_next = out_degree_map.get(next_node);
						
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

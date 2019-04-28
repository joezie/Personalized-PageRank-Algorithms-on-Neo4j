package joezie.fora_neo4j;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import org.neo4j.register.Register.Int;

public class Fora_Topk extends Algo_Util implements Topk_Util_Interface { // FORA algorithm in top-k query
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph	
	private HashMap<Long, Double> residue; // propagated value, i.e r
	private HashMap<Long, Double> reserve; // stored value, i.e pi
	private HashMap<Long, Double> topk_res; // top-k ppr result
	private Vector<Long> topk_nodeIds; // top-k node ids sorted by ppr
	//HashMap<Node, Integer> out_degree_map; // record the outgoing degree for each node
	//HashMap<Node, Iterable<Relationship>> rel_iter_map; // record the outgoing relationship iterable for each node
		
	// tag::configuration parameters[]
	private int node_amount; // the total number of nodes in the graph
	private int rel_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	//private Double rmax; // the residue(r) threshold for local update
	//private Double min_rmax; // the minimum residue(r) threshold for local update
	private Double rsum; // the sum of all nodes' residues(r) during a local update process from s
	//private Double epsilon; // error bound
	private Double pfail; // failure probability
	private Double delta; // the reserve(pi) threshold
	private Double min_delta; // the minimum reserve(pi) threshold
	//private Double omega; // the total number of random walks
	private int k;
	// end::configuration parameters[]
	
	public Fora_Topk(Double alpha, Double rsum, Double pfail, Double delta, 
			int node_amount, int rel_amount, GraphDatabaseService graphDb, Double min_delta, 
			int k, Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		topk_res = new HashMap<>();
		topk_nodeIds = new Vector<>();
		//this.out_degree_map = out_degree_map;
		//this.rel_iter_map = rel_iter_map;
		
		// tag::configuration parameters assignment[]
		this.node_amount = node_amount;
		this.rel_amount = rel_amount;
		this.alpha = alpha;
		//this.rmax = rmax;
		//this.min_rmax = min_rmax;
		this.rsum = rsum;
		//this.epsilon = epsilon;
		this.pfail = pfail;
		this.delta = delta;
		this.min_delta = min_delta;
		this.k = k;
		//omega = (epsilon + 2.0) * Math.log(2.0 / pfail) / epsilon / epsilon / delta;
		// Note: omega hasn't times rsum yet (omega needs to times rsum after forward push)
		// end::configuration parameters assignment[]
	}
	
	/*
	public int getK() { // return k in top-k
		return k;
	}
	*/
	
	@Override
	public void printTopKResult(int dummy) {
		if (topk_res.isEmpty())
			retrieveTopK(k);
		//computeTopK();
		
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(topk_res.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		int count = 0;
		System.out.println("\nFora-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
		}
	}
	
	/*
	@Override
	public HashMap<Long, Double> getTopKPPR(int dummy) { // return the reference of topk_res; might include more than k nodes
		//computeTopK();
		return topk_res;
	}
	*/
	
	@Override
	public Vector<Long> getTopKNodeIds(int dummy) { // return the reference of topk_nodeIds sorted by ppr; might include more than k nodes
		if (!topk_nodeIds.isEmpty()) // topk_node_ids is already set, then no need to compute again 
			return topk_nodeIds;
		
		if (topk_res.isEmpty())
			retrieveTopK(k);
		//computeTopK();
		
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

	/*
	private Boolean computeTopK() { // get the top-k results and store in topk_res; might include more than k nodes
		if (!topk_res.isEmpty()) // topk_res is already set, then no need to compute again 
			return true;
		
		Double kth_reserve = kth_ppr(reserve.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			//System.out.println("Fora Top-k: there might be less than k ppr results");
			topk_res = new HashMap<>(reserve);
			return false;
		}
			
		for (Map.Entry<Long, Double> reserve_t : reserve.entrySet()) {
			// find the pprs not smaller than the kth ppr, so it's possible that
			// the size of topk_res is greater than k
			if (reserve_t.getValue() >= kth_reserve)
				topk_res.put(reserve_t.getKey(), reserve_t.getValue());
		}
		return true;
	}
	*/

	@Override
	public void computeTopKPPR(Long nodeId_start, int dummy, Object eps) {
		// clear results of last call
		residue.clear();
		reserve.clear();
		topk_res.clear();
		topk_nodeIds.clear();

		Double epsilon = (Double)eps;
		epsilon *= 0.5; //epsilon' = epsilon / 2
		Double delta_local = delta, 
				omega_local = (epsilon + 2.0) * Math.log(2.0 / pfail) / epsilon / epsilon / delta,
				//rmax_local = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail)),
				min_rmax = epsilon * Math.sqrt(min_delta / 3 / rel_amount / Math.log(2 / pfail)),
				rsum_local = rsum;

		//rmax_local *= Math.sqrt((double)rel_amount * rmax_local) * 3.0;


		long startTime = 0, endTime = 0, duration_fwdpush = 0, duration_random = 0;
		Queue<Long> Q = new ConcurrentLinkedQueue<>();
		Q.offer(nodeId_start);
		boolean isFirstPwdpush = true; // mark the first forward push
		int round = 0;
		Forward_Push fp_section = new Forward_Push(alpha, rsum_local, node_amount, graphDb, adjM, node_property, dir_db);

		while (delta_local >= min_delta) {
			/*
			// debug info
			round++;
			System.out.println("Fora-TopK:Round " + round + "\tdelta = " + delta_local);
			*/

			Double rmax_local = epsilon * Math.sqrt(delta_local / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
			omega_local = (epsilon + 2.0) * Math.log(2.0 / pfail) / epsilon / epsilon / delta_local;

			// tag::new version[]
			//int out_neighbor_start = out_degree_map.get(start);
			int nodeIdM_start = adjM.toMappedNodeId(nodeId_start); // start node id in adjacency matrix
			int out_degree_start = adjM.degree(nodeIdM_start, Direction.OUTGOING);
			if (out_degree_start == 0) { // terminate if start node's out degree is 0
				reserve.put(nodeId_start, 1.0);
				rsum_local = 0.0;
				break;
			}
			rmax_local *= Math.sqrt((double)rel_amount * rmax_local) * 3.0;
			// end::new version[]

			//fp_section.setRmax(rmax_local);

			// part 1: perform forward push
			startTime = System.nanoTime();
			/**/
			//try ( Transaction tx = graphDb.beginTx() ) {
			fp_section.forward_push_topk(nodeId_start, Q, min_rmax, isFirstPwdpush, rmax_local);
			//	tx.success();
			//}
			endTime = System.nanoTime();
			duration_fwdpush += (endTime - startTime);

			isFirstPwdpush = false;
			rsum_local = fp_section.getUpdatedRsum();
			reserve = fp_section.getReserveCopy();
			residue = fp_section.getResidueCopy();
			Q = fp_section.getNextQueueCopy(); // update Q

			// part 2: perform random walks

			// tag::new version[]
			Double rsum_random_walk = rsum_local * (1.0 - alpha); //rsum used by random walk
			// end::new version[]

			Monte_Carlo mc_section = new Monte_Carlo(alpha, node_amount, graphDb, pfail, delta_local, adjM, node_property, dir_db);
			long num_random_walk = (long)(omega_local * rsum_random_walk);


			startTime = System.nanoTime();
			/**/
			//try ( Transaction tx = graphDb.beginTx() ) {
			for (Map.Entry<Long, Double> entry : residue.entrySet()) { // perform random walk from each v_i for omega_i times
				Long nodeId_cur = entry.getKey();
				Double residue_cur = entry.getValue();

				//long omega_i = (long)Math.ceil(residue_cur / rsum * num_random_walk);
				//Double a_i = residue_cur / rsum * num_random_walk / omega_i;
				//Double reserve_incr = a_i / num_random_walk * rsum;

				// tag::new version[]
				long omega_i = (long)Math.ceil(residue_cur * (double)num_random_walk);
				Double a_i = residue_cur * (double)num_random_walk / (double)omega_i;
				Double reserve_incr = a_i / (double)num_random_walk;
				// end::new version[]

				for (long j = 0; j < omega_i; j++) {
					Long nodeId_dest = mc_section.random_walk(nodeId_cur);
					Double old_reserve_dest = reserve.get(nodeId_dest);
					if (old_reserve_dest == null)
						old_reserve_dest = 0.0;
					reserve.put(nodeId_dest, old_reserve_dest + reserve_incr);
				}
			}
			//tx.success();
			//}
			endTime = System.nanoTime();
			duration_random += (endTime - startTime);

			// part 3: check termination conditions

			//if (can_terminate() || delta <= min_delta)
			//		break;
			//else
			//		delta = Math.max(min_delta, delta / 2.0);

			// tag::new version[]
			Double kth_ppr_score = kth_ppr(reserve.values().toArray(), k);
			kth_ppr_score = (kth_ppr_score == null) ? 0.0 : kth_ppr_score;
			if (kth_ppr_score >= (1 + epsilon) * delta_local || delta_local <= min_delta)
				break;
			else
				delta_local = Math.max(min_delta, delta_local / 4.0); // divide faster: divided by 4 instead of 2
			// tag::new version[]

		}
		//System.out.println("Finish forward push in " + duration_fwdpush / 1000000 + "(ms)");
		//System.out.println("Finish random walk in " + duration_random / 1000000 + "(ms)");
		
		/*
		Double kth_reserve = kth_ppr(reserve.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			System.out.println("Fora Top-k: there might be less than k ppr results");
			topk_res = new HashMap<>(reserve);
			return;
		}
			
		for (Map.Entry<Long, Double> reserve_t : reserve.entrySet()) {
			// find the pprs not smaller than the kth ppr, so it's possible that
			// the size of topk_res is greater than k
			if (reserve_t.getValue() >= kth_reserve)
				topk_res.put(reserve_t.getKey(), reserve_t.getValue());
		}
		*/
	}
	
	private void retrieveTopK(int k) { // retrieve top-k results from whole-graph ppr results and store in topk_res
		Double kth_reserve = kth_ppr(reserve.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			//System.out.println("Fora Top-k: there might be less than k ppr results");
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
}

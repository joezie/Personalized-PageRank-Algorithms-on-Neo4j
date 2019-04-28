package joezie.fora_neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.security.AccessMode.Static;
import org.neo4j.register.Register.Int;

public class Monte_Carlo extends Algo_Util implements Whole_Graph_Util_Interface, Preprocessing_Interface, Topk_Util_Interface { // Monte-Carlo algorithm based on random walks
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph	
	private HashMap<Long, Double> ppr; // (nodeId in graphDb, ppr value)
	//HashMap<Node, Integer> out_degree_map; // record the outgoing degree for each node
	//HashMap<Node, Iterable<Relationship>> rel_iter_map; // record the outgoing relationship iterable for each node
		
	// tag::configuration parameters[]
	private int node_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	//private Double omega; // the total number of random walks
	//private Double epsilon; // error bound
	private Double delta; // the reserve(pi) threshold
	private Double pfail; // failure probability
	// end::configuration parameters[]
	
	// tag::timing parameters[]
	public long startTime, endTime, duration;
	// end::timing parameters[]
	private String preprocessing_dirName;
	private Vector<Long> topk_nodeIds; // top-k node ids sorted by ppr
	private HashMap<Long, Double> topk_res; // top-k ppr result
	
	public Monte_Carlo(Double alpha, int node_amount, GraphDatabaseService graphDb,
			Double pfail, Double delta, Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		//this.out_degree_map = out_degree_map;
		//this.rel_iter_map = rel_iter_map;
		ppr = new HashMap<>();
		topk_nodeIds = new Vector<>();
		topk_res = new HashMap<>();
		
		// tag::configuration parameters assignment[]
		this.node_amount = node_amount;
		this.alpha = alpha;
		//this.epsilon = epsilon;
		this.pfail = pfail;
		this.delta = delta;
		//omega = 3 * Math.log(2 / pfail) / epsilon / epsilon / delta;
		// end::configuration parameters assignment[]
		
		preprocessing_dirName = "MC_ppr_results/" + dir_db;
	}
	
	public Long random_walk(Long nodeId_start) { 
		/* Return the final node where the random walk from start node stops at.
		 * During the random walk, it would stop at a node with a probability of
		 * alpha, and move to a randomly picked outgoing neighboring node with a
		 * probability of 1 - alpha.
		 */
			
		/*
		int out_degree_start = 0;
		try ( Transaction tx = graphDb.beginTx() ) {
			out_degree_start = start.getDegree(Direction.OUTGOING);
			tx.success();
		}
		*/
		
		int nodeIdM_start = adjM.toMappedNodeId(nodeId_start); // start node id in adjacency matrix
		//int out_degree_start = out_degree_map.get(start);
		int out_degree_start = adjM.degree(nodeIdM_start, Direction.OUTGOING);
		
		if (out_degree_start == 0)
			// if there's no outgoing edge from this node, then return the start node
			return nodeId_start;
		
		//try ( Transaction tx = graphDb.beginTx() ) {
		int nodeIdM_cur = nodeIdM_start; // current node id in adjacency matrix
		while (true) {
			if (ThreadLocalRandom.current().nextDouble(1.0) < alpha)
				// Stop at current node with a probability of alpha
				//return cur_node;
				break;
						
			//int out_degree_cur = out_degree_map.get(cur_node);
			int out_degree_cur = adjM.degree(nodeIdM_cur, Direction.OUTGOING);
			if (out_degree_cur > 0) {
				// At least one outgoing neighboring node exists, then randomly
				// pick an outgoing neighboring node as the next node
				int picked_rel_num = ThreadLocalRandom.current().nextInt(out_degree_cur);
				//Iterator<Relationship> iter = rel_iter_map.get(cur_node).iterator();
				
				//while (picked_rel_num > 0) {
				//	iter.next();
				//	picked_rel_num--;
				//}
				//Relationship picked_rel = iter.next();
				//cur_node = picked_rel.getOtherNode(cur_node);
				nodeIdM_cur = adjM.getTarget(nodeIdM_cur, picked_rel_num, Direction.OUTGOING); // move to the node at outgoing[nodeIdM_cur][picked_rel_num]
			}
			else {
				// If there's no outgoing edge from this node, then reset current node to the start node
				nodeIdM_cur = nodeIdM_start;
			}
		}
		Long nodeId_cur = adjM.toOriginalNodeId(nodeIdM_cur); // current node id in graphDb 
		return nodeId_cur;
	}
	
	// tag::new version[]
	public Long random_walk_no_zero_hop(Long nodeId_start) { 
		/* Return the final node where the random walk from start node stops at.
		 * During the random walk, it would stop at a node with a probability of
		 * alpha, and move to a randomly picked outgoing neighboring node with a
		 * probability of 1 - alpha.
		 */	
		//int out_degree_start = out_degree_map.get(start);
		int nodeIdM_start = adjM.toMappedNodeId(nodeId_start); // start node id in adjacency matrix
		int out_degree_start = adjM.degree(nodeIdM_start, Direction.OUTGOING);

		
		if (out_degree_start == 0)
			// if there's no outgoing edge from this node, then return the start node
			return nodeId_start;
		
				
		// tag::new version[]
		
		 // move a step from start to one of its neighbor
		int picked_rel_num_start = ThreadLocalRandom.current().nextInt(out_degree_start);
		//Iterator<Relationship> iter_start = rel_iter_map.get(start).iterator();
		//while (picked_rel_num_start > 0) {
		//	iter_start.next();
		//	picked_rel_num_start--;
		//}
		//Relationship picked_rel_start = iter_start.next();
		//cur_node = picked_rel_start.getOtherNode(cur_node);				
		int nodeIdM_cur = adjM.getTarget(nodeIdM_start, picked_rel_num_start, Direction.OUTGOING); // current node id in adjacency matrix
		
		while (true) {
			if (ThreadLocalRandom.current().nextDouble(1.0) < alpha)
				// Stop at current node with a probability of alpha
				//return cur_node;
				break;
						
			//int out_degree_cur = out_degree_map.get(cur_node);
			int out_degree_cur = adjM.degree(nodeIdM_cur, Direction.OUTGOING);
			if (out_degree_cur > 0) {
				// At least one outgoing neighboring node exists, then randomly
				// pick an outgoing neighboring node as the next node
				int picked_rel_num = ThreadLocalRandom.current().nextInt(out_degree_cur);
				//Iterator<Relationship> iter = rel_iter_map.get(cur_node).iterator();
				
				//while (picked_rel_num > 0) {
				//	iter.next();
				//	picked_rel_num--;
				//}
				//Relationship picked_rel = iter.next();
				//cur_node = picked_rel.getOtherNode(cur_node);
				nodeIdM_cur = adjM.getTarget(nodeIdM_cur, picked_rel_num, Direction.OUTGOING); // move to the node at outgoing[nodeIdM_cur][picked_rel_num]
			}
			else {
				// If there's no outgoing edge from this node, then reset current node to the start node
				nodeIdM_cur = nodeIdM_start;
			}
		}
		Long nodeId_cur = adjM.toOriginalNodeId(nodeIdM_cur); // current node id in graphDb 
		return nodeId_cur;
	}
	// end::new version[]
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object epsilon) {
		/* Perform random walk from start node for omega times. Then estimate nodes'
		 * ppr value as the ratio of random walks stopping at it. 
		 */
		
		//try ( Transaction tx = graphDb.beginTx() ) {
		ppr.clear();
		topk_nodeIds.clear();
		topk_res.clear();
		
		Double omega = 3 * Math.log(2 / pfail) / (Double)epsilon / (Double)epsilon / delta;
		
		HashMap<Long, Integer> stop_count = new HashMap<>();
			//Iterator<Node> iter = graphDb.getAllNodes().iterator();
			//while (iter.hasNext()) 
			//	stop_count.put(iter.next(), 0);
		/*
		adjM.forEachNode(nodeIdM -> {
			Long nodeId = adjM.toOriginalNodeId(nodeIdM);
			stop_count.put(nodeId, 0);
			return true;
		});
		*/
		
		for (long i = 1; i <= omega; i++) {
			//Long nodeId_final = random_walk_no_zero_hop(nodeId_start);
			Long nodeId_final = random_walk(nodeId_start);
			Integer old_stop_count = stop_count.get(nodeId_final);
			old_stop_count = (old_stop_count == null) ? 0 : old_stop_count;
			stop_count.put(nodeId_final, old_stop_count + 1);
		}
		
		for (Map.Entry<Long, Integer> stop_count_t : stop_count.entrySet()) 
			ppr.put(stop_count_t.getKey(), ((double)(stop_count_t.getValue())) / omega);
		
        
        
			//iter = graphDb.getAllNodes().iterator(); // rewind iter
			//while (iter.hasNext()) {
			//	Node node = iter.next();
			//	ppr.put(node, (double)stop_count.get(node) / omega);
			//}
		/*
		adjM.forEachNode(nodeIdM -> {
			Long nodeId = adjM.toOriginalNodeId(nodeIdM);
			ppr.put(nodeId, (double)stop_count.get(nodeId) / omega);
			return true;
		});
		*/
			//tx.success();
		//}
	}
	
	@Override
	public void printWholeGraphResult() {
		/*
		//Iterator<Node> iter = null;
		//try ( Transaction tx = graphDb.beginTx() ) {
		//	iter = graphDb.getAllNodes().iterator();
		//	tx.success();
		//}
		System.out.println("Monte-Carlo PPR:");
		//while (iter.hasNext()) {
		//	Node node = iter.next();
		//	System.out.println("@" + getNodeName(node, graphDb) + '\t' + ppr.get(node));
		//}
		adjM.forEachNode(nodeIdM -> {
			Long nodeId = adjM.toOriginalNodeId(nodeIdM);
			System.out.println("@" + getNodeName(nodeId, graphDb) + '\t' + ppr.get(nodeId));
			return true;
		});
		*/
		
		
		
		List<Map.Entry<Long, Double>> reserve_list = new ArrayList<Map.Entry<Long, Double>>(ppr.entrySet());
		reserve_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		
		System.out.println("Monte-Carlo PPR:");
		for (Map.Entry<Long, Double> reserve_t : reserve_list) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue());
	}

	@Override
	public HashMap<Long, Double> getWholeGraphPPR() { // return the reference of reserve
		return ppr;
	}

	@Override
	public void preprocessing(Double dummy, Object epsilon) {
		// customize preprocessing_dirName
		//preprocessing_dirName += ("/" + threshold + "_" + epsilon);
		preprocessing_dirName += ("/" + epsilon);
		
		System.out.println("\nMonte-Carlo preprocessing started...");
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
			computeWholeGraphPPR(nodeId_start, epsilon); // run Monte Carlo on src node
			
			String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
			try { // store ppr values that are not smaller than threshold
				FileWriter fw = new FileWriter(fileName_start, false);
				for (Map.Entry< Long, Double > entry_t : ppr.entrySet()) {
					Long nodeId_i = entry_t.getKey();
					Double ppr_i = entry_t.getValue();
					//if (ppr_i >= threshold)
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
		ppr.clear();
		String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(fileName_start)));
			String tmpStr = null;
			while ((tmpStr = br.readLine()) != null) {
				int space_idx = tmpStr.indexOf('\t');
				Long nodeId_target = Long.parseLong(tmpStr.substring(0, space_idx));
				Double ppr_target = Double.parseDouble(tmpStr.substring(space_idx + 1));
				ppr.put(nodeId_target, ppr_target);
			}
			br.close();
		}
		catch (IOException e) {
			System.out.println("Read from file " + fileName_start + " failed!");
			e.printStackTrace();
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
	
	@Override
	public void computeTopKPPR(Long nodeId_start, int k, Object epsilon) {
		//if (!topk_res.isEmpty()) // topk_res is already set, then no need to compute again 
		//	return true;
		
		// 1. perform Monte-Carlo Whole-Graph SSPPR algo
		computeWholeGraphPPR(nodeId_start, epsilon);
		
		/*
		// 2. retrieve top-k results and store in topk_res 
		Double kth_reserve = kth_ppr(ppr.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			//System.out.println("Power Method: there might be less than k ppr results");
			topk_res = new HashMap<>(ppr);
			return;
		}
			
		for (Map.Entry<Long, Double> reserve_t : ppr.entrySet()) {
			// find the pprs not smaller than the kth ppr, so it's possible that
			// the size of topk_res is greater than k
			if (reserve_t.getValue() >= kth_reserve)
				topk_res.put(reserve_t.getKey(), reserve_t.getValue());
		}
		return;
		*/
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
		System.out.println("\nMonte-Carlo-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
		}
	}
	
	private void retrieveTopK(int k) { // retrieve top-k results from whole-graph ppr results and store in topk_res
		if (!topk_res.isEmpty())
			return;
		
		Double kth_reserve = kth_ppr(ppr.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
			//System.out.println("Power Method: there might be less than k ppr results");
			topk_res = new HashMap<>(ppr);
			return;
		}

		for (Map.Entry<Long, Double> reserve_t : ppr.entrySet()) {
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
	
	/*
	@Override
	public Object getPreprocessedPPR() {
		return ppr;
	}
	*/
	
	/*
	void test() {
		try ( Transaction tx = graphDb.beginTx() )
        {
			int outdeg = nodes[2].getDegree(Direction.OUTGOING);
			Iterator<Relationship> iter = nodes[2].getRelationships(Direction.OUTGOING).iterator();
			System.out.println("Out degree:" + outdeg);
			//for (Relationship rel : rels)
			while (iter.hasNext()) {
				Relationship rel = iter.next();
				System.out.println("@" + rel.getOtherNode(nodes[2]).getProperty("Name"));
			}
            tx.success();
        }
	}
	*/
	/*
	private String getNodeName(Node node) {
		String nodeName = "";
		try ( Transaction tx = graphDb.beginTx() ) {
			nodeName = node.getProperties("Name").toString();		
			tx.success();		
		}
		return nodeName;
	}
	*/
	/*
	public static void main( final String[] args ) throws IOException {
		Monte_Carlo mc_t = new Monte_Carlo(0.15, 0.01);
		mc_t.createDb();
		mc_t.construct_graph();
		mc_t.monte_carlo(mc_t.nodes[0]);
		mc_t.printPPR();
		mc_t.shutDown();
	}
	*/
}

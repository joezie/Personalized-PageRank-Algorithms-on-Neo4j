package joezie.fora_neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.cypher.internal.compiler.v3_1.pipes.ProcedureCallRowProcessing;
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.formatErrors;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.GraphDatabaseService;

import io.netty.handler.codec.http2.StreamByteDistributor.Writer;

public class Base_Whole_Graph extends Algo_Util implements Whole_Graph_Util_Interface, Preprocessing_Interface, Topk_Util_Interface { 
	// All-Pair-Backward-Search algorithm for All-Pair PPR (i.e preprocessing) computation
	
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph 
	private HashMap< Long, LinkedHashMap< Long, Double > > ppr_all_pairs; // (v, (t, pi(v, t)))
	private LinkedHashMap< Long, Double > ppr_src; // ppr of all nodes in respect to src node (LinkedHashMap would keep entries' insertion order)
	private int node_amount; // the total number of nodes in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	private String preprocessing_dirName;
	
	public Base_Whole_Graph(Double alpha, int node_amount, GraphDatabaseService graphDb,
			Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		ppr_all_pairs = new HashMap<>();
		ppr_src = new LinkedHashMap<>();
		this.node_amount = node_amount;
		this.alpha = alpha;
		preprocessing_dirName = "BASE_ppr_results/" + dir_db;
	}
	
	@Override
	public void preprocessing(Double threshold, Object k) { 
		/* Preprocessing using Backward Search on each node as target node;
		 * if it's preprocessing for top-k, then we only store top-k ppr of each src node
		 * if k is negative, it indicates a preprocessing for whole-graph 
		 */
		
		// customize preprocessing_dirName
		preprocessing_dirName += ("/" + threshold + "_" + k);
		
		System.out.println("\nBASE preprocessing starts...");
		Backward_Search bws_t = new Backward_Search(alpha, threshold, node_amount, graphDb, adjM);
		HashMap<Integer, Integer> prog_pct_map = new HashMap<>(); // (nodeId, progress percentage)
		for (int i = 10; i < 100; i += 10) {
			int nodeId_i = node_amount * i / 100;
			prog_pct_map.put(nodeId_i, i);
		}
		prog_pct_map.put(node_amount - 1, 100);
		
		adjM.forEachNode(nodeIdM -> {
			Long nodeId_target = adjM.toOriginalNodeId(nodeIdM);
			bws_t.backward_search_whole_graph(nodeId_target); // run Backward Search on target node
			HashMap<Long, Double> reserve = bws_t.getReserve(); // get reserve results (v, pi(v, t))
			for (Map.Entry<Long, Double> reserve_t : reserve.entrySet()) { // store results in ppr
				Long nodeId_v = reserve_t.getKey();
				Double pi_v = reserve_t.getValue();
				if (pi_v >= threshold) { // only insert reserve >= rmax
					if (ppr_all_pairs.containsKey(nodeId_v) == false)
						ppr_all_pairs.put(nodeId_v, new LinkedHashMap< Long, Double >());
					ppr_all_pairs.get(nodeId_v).put(nodeId_target, pi_v); // (v, t, pi(v, t))
				}
			}
			if (prog_pct_map.containsKey(nodeIdM))
				System.out.println("Progress: " + prog_pct_map.get(nodeIdM) + "%");
			return true;
		});

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
		
		System.out.println("\nBASE preprocessing storing results to files (under directory " + preprocessing_dirName + ")...");
		// sort each map (if required), and store into files
		for (Map.Entry< Long, LinkedHashMap< Long, Double > > ppr_t : ppr_all_pairs.entrySet()) {
			Long nodeId_v = ppr_t.getKey(); // also as the name of output file
			LinkedHashMap< Long, Double > ppr_to_targets = ppr_t.getValue();
			ArrayList< SimpleEntry< Long, Double > > sorted_topk_res = new ArrayList< SimpleEntry< Long, Double > >();
			
			
			String fileName_v = preprocessing_dirName + "/" + nodeId_v.toString() + ".txt";
			
			if ((int)k < 0) { // preprocessing for whole-graph
				try {
					FileWriter fw = new FileWriter(fileName_v, false);
					for (Map.Entry< Long, Double > entry_t : ppr_to_targets.entrySet()) {
						fw.write(entry_t.getKey().toString() + '\t' + entry_t.getValue().toString() + '\n');
					}
					fw.close();
				}
				catch (IOException e) {
					System.out.println("Write to file " + fileName_v + " failed!");
					e.printStackTrace();
				}
			}
			else { // preprocessing for top-k, then we would only retrive the top-k results and sort
				Double kth_reserve = kth_ppr(ppr_to_targets.values().toArray(), (int)k); // find the kth ppr
				
				for (Map.Entry<Long, Double> reserve_t : ppr_to_targets.entrySet()) {
					// 1. If there're less than k results, then we copy all of them to sorted_topk_res
					// 2. Otherwise, find the pprs not smaller than the kth ppr, so it's possible that
					//    the size of topk_res is greater than k;
					if (kth_reserve == null || reserve_t.getValue() >= kth_reserve)
						sorted_topk_res.add(new SimpleEntry<Long, Double>(reserve_t.getKey(), reserve_t.getValue()));
				}
				sorted_topk_res.sort( new Comparator< SimpleEntry< Long, Double > >() { // sort in descending order
					public int compare(SimpleEntry< Long, Double > k1, SimpleEntry< Long, Double > k2) { 
						// overload with k2.val > k1.val
						return k2.getValue().compareTo(k1.getValue());
					}
				} );
				
				
				try {
					FileWriter fw = new FileWriter(fileName_v, false);
					for (Map.Entry< Long, Double > entry_t : sorted_topk_res) {
						fw.write(entry_t.getKey().toString() + '\t' + entry_t.getValue().toString() + '\n');
					}
					fw.close();
				}
				catch (IOException e) {
					System.out.println("Write to file " + fileName_v + " failed!");
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object dummy) { // read pprs from file corresponding to src node
		ppr_all_pairs.clear();
		ppr_src.clear();
		String fileName_v = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(fileName_v)));
			String tmpStr = null;
			while ((tmpStr = br.readLine()) != null) {
				int space_idx = tmpStr.indexOf('\t');
				Long nodeId_target = Long.parseLong(tmpStr.substring(0, space_idx));
				Double ppr_target = Double.parseDouble(tmpStr.substring(space_idx + 1));
				ppr_src.put(nodeId_target, ppr_target);
			}
			br.close();
		}
		catch (IOException e) {
			System.out.println("Read from file " + fileName_v + " failed!");
			e.printStackTrace();
		}
	}

	@Override
	public void printWholeGraphResult() {
		System.out.println("\nBase-Whole-Graph PPR:");
		for (Map.Entry<Long, Double> reserve_t : ppr_src.entrySet()) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue());
	}

	@Override
	public HashMap<Long, Double> getWholeGraphPPR() { // return LinkedHashMap ppr_src
		return ppr_src;
	}

	@Override
	public void readPreprocessedPPR(Long nodeId_start) {
		computeWholeGraphPPR(nodeId_start, null);
		return;
	}

	@Override
	public Vector<Long> getTopKNodeIds(int k) { 
		// return the reference of topk_nodeIds sorted by ppr; might include more than k nodes
		return new Vector<>(ppr_src.keySet());
	}
	
	@Override
	public void computeTopKPPR(Long nodeId_start, int dummy1, Object dummy2) { // store the top-k results in topk_res	
		computeWholeGraphPPR(nodeId_start, dummy2); // perform BASE Whole-Graph SSPPR algo
		
		return;
	}
	
	@Override
	public void printTopKResult(int k) {
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(ppr_src.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		int count = 0;
		System.out.println("\nBASE-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
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

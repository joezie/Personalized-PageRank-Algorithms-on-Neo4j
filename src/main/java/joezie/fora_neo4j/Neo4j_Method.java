package joezie.fora_neo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.internal.compiler.v2_3.docgen.plannerDocGen.idNameConverter;
import org.neo4j.function.ThrowingBooleanSupplier;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.PageRankAlgorithm;
import org.neo4j.graphalgo.impl.PageRankResult;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.BufferingLog;
import org.neo4j.logging.Log;


public class Neo4j_Method extends Algo_Util implements Whole_Graph_Util_Interface, Preprocessing_Interface, Topk_Util_Interface { // Calling PPR function provided by Neo4j Graphalgo package
	private GraphDatabaseService graphDb;
	private String dir_db;
	private Graph adjM; // adjacency matrix of the graph
	private int node_amount; // the total number of nodes in the graph
	private Double alpha;
	private HashMap<Long, Double> ppr;
	private String label_type;
	private String rel_type;
	private PageRankResult rankResult;
	private String preprocessing_dirName;
	private Vector<Long> topk_nodeIds; // top-k node ids sorted by ppr
	private HashMap<Long, Double> topk_res; // top-k ppr result
	
	public Neo4j_Method(GraphDatabaseService graphDb, Double alpha, String label_type, 
			String rel_type, Graph adjM, int node_amount, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		this.alpha = alpha;
		this.label_type = label_type;
		this.rel_type = rel_type;
		this.node_amount = node_amount;
		ppr = new HashMap<>();
		topk_nodeIds = new Vector<>();
		topk_res = new HashMap<>();
		rankResult = null;
		preprocessing_dirName = "Neo4j_Method_ppr_results/" + dir_db;
	}
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object iterations) {
		ppr.clear();
		topk_nodeIds.clear();
		topk_res.clear();
		
		rankResult = null;
		LongStream nodeId_start_stream = LongStream.of(nodeId_start);
		rankResult = PageRankAlgorithm
                .of(adjM, 1 - alpha, nodeId_start_stream, Pools.DEFAULT, 2, 1) // Note: alpha (damping factor) in Neo4j is different from ours
                .compute((int)iterations)
                .result();
	}
	
	private Boolean buildPPRMap() { // build ppr based on rankResult	
		Double ppr_sum = 0.0;	
		for (int i = 0; i < node_amount; i++)
			ppr_sum += rankResult.score(i);
		
		if (ppr_sum == 0.0) {
			System.out.println("Error: sum of ppr equials 0!");
			return false;
		}
		
		System.out.println("ppr sum: " + ppr_sum);
		
		for (int i = 0; i < node_amount; i++) {
			long nodeId = adjM.toOriginalNodeId(i);
			Double ppr_i = rankResult.score(i);
			if (ppr_i > 0.0) // we don't store ppr whose value is 0
				ppr.put(nodeId, ppr_i / ppr_sum); // normalization
		}
		return true;
	}
	
	@Override
	public void printWholeGraphResult() {
		if (ppr.isEmpty())
			buildPPRMap();
		List<Map.Entry<Long, Double>> reserve_list = new ArrayList<Map.Entry<Long, Double>>(ppr.entrySet());
		reserve_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		
		System.out.println("Neo4j-Method-PPR:");
		for (Map.Entry<Long, Double> reserve_t : reserve_list) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue());		
	}

	@Override
	public HashMap<Long, Double> getWholeGraphPPR() {
		if (ppr.isEmpty())
			buildPPRMap();
		return ppr;
	}
	
	@Override
	public void preprocessing(Double dummy, Object iterations) {
		// customize preprocessing_dirName
		preprocessing_dirName += ("/" + iterations);
		
		System.out.println("\nNeo4j-Method preprocessing started...");
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
			computeWholeGraphPPR(nodeId_start, iterations); // run Neo4j Method on src node
			
			String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
			try { // store ppr values that are not smaller than threshold
				FileWriter fw = new FileWriter(fileName_start, false);
				
				Double ppr_sum = 0.0;	
				for (int i = 0; i < node_amount; i++)
					ppr_sum += rankResult.score(i);
				
				if (ppr_sum == 0.0) {
					System.out.println("Error: sum of ppr equials 0!");
				}
					
				for (int i = 0; i < node_amount; i++) {
					Long nodeId_i = adjM.toOriginalNodeId(i);
					Double ppr_i = rankResult.score(i) / ppr_sum; // normalization
					try {
	            		fw.write(nodeId_i.toString() + '\t' + ppr_i.toString() + '\n');
	            	}
	            	catch (IOException e) {
	            		System.out.println("Write to file " + fileName_start + " failed!");
	            		e.printStackTrace();
	            	}
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
	public Vector<Long> getTopKNodeIds(int k) { 
		// return the reference of topk_nodeIds sorted by ppr; might include more than k nodes
		
		if (!topk_nodeIds.isEmpty()) // topk_node_ids is already set, then no need to compute again 
			return topk_nodeIds;		

		if (ppr.isEmpty())
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
	public void computeTopKPPR(Long nodeId_start, int k, Object iterations) { 
		// store the top-k results in topk_res; might include more than k nodes
		
		computeWholeGraphPPR(nodeId_start, iterations);
		// perform Neo4j Method Whole-Graph SSPPR algo
	}
	
	@Override
	public void printTopKResult(int k) {
		if (ppr.isEmpty())
			retrieveTopK(k);
		
		List<Map.Entry<Long, Double>> topk_res_list = new ArrayList<Map.Entry<Long, Double>>(topk_res.entrySet());
		topk_res_list.sort( new Comparator<Map.Entry<Long, Double>>() { // sort in descending order
			public int compare(Map.Entry<Long, Double> k1, Map.Entry<Long, Double> k2) { 
				// overload with k2.val > k1.val
				return k2.getValue().compareTo(k1.getValue());
			}
		} );
		int count = 0;
		System.out.println("\nNeo4j-Method-Top" + k + " PPR:");
		for (Map.Entry<Long, Double> topk_res_list_t : topk_res_list) {
			if (count >= k) // we need to count since the size of topk_reserves might be greater than k
				break;
			count++;
			System.out.println("@" + getNodeName(topk_res_list_t.getKey(), graphDb) + '\t' + topk_res_list_t.getValue());
		}
	}
	
	private void retrieveTopK(int k) { // retrieve top-k results from whole-graph ppr results and store in topk_res
		// 1. build ppr map
		if (ppr.isEmpty())
			buildPPRMap();
		
		// 2. retrieve top-k results and store in topk_res 
		Double kth_reserve = kth_ppr(ppr.values().toArray(), k); // find the kth ppr
		if (kth_reserve == null) { // there might be less than k results, so we copy all of them to topk_res
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

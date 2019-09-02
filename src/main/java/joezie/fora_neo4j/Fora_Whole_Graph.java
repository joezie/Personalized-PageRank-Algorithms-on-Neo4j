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

import org.apache.commons.io.FileUtils;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.register.Register.Int;

public class Fora_Whole_Graph extends Algo_Util implements Whole_Graph_Util_Interface, Preprocessing_Interface { // FORA algorithm in whole graph query
	private GraphDatabaseService graphDb;
	private String dir_db;
	Graph adjM; // adjacency matrix of the graph	
	private HashMap<Long, Double> residue; // propagated value, i.e r
	private HashMap<Long, Double> reserve; // stored value, i.e pi
	private int node_amount; // the total number of nodes in the graph
	private int rel_amount; // the total number of edges in the graph
	private Double alpha; // the probability stopped at each node during a random walk
	private Double rsum; // the sum of all nodes' residues(r) during a local update process from s
	private Double pfail; // failure probability
	private Double delta; // the reserve(pi) threshold
	private static final Double avg_rand_walk_time = 400.0; // 400(ns)
	private String preprocessing_dirName;
	
	public Fora_Whole_Graph(Double alpha, Double rsum, Double pfail, Double delta, int node_amount, int rel_amount,
			GraphDatabaseService graphDb, Graph adjM, String node_property, String dir_db) {
		super(node_property);
		this.graphDb = graphDb;
		this.dir_db = dir_db;
		this.adjM = adjM;
		residue = new HashMap<>();
		reserve = new HashMap<>();
		this.node_amount = node_amount;
		this.rel_amount = rel_amount;
		this.alpha = alpha;
		this.rsum = rsum;
		this.pfail = pfail;
		this.delta = delta;
		preprocessing_dirName = "FORA_ppr_results/" + dir_db;
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
		
		System.out.println("Fora-Whole-Graph PPR:");
		for (Map.Entry<Long, Double> reserve_t : reserve_list) 
			System.out.println("@" + getNodeName(reserve_t.getKey(), graphDb) + '\t' + reserve_t.getValue());
	}
	
	@Override
	public HashMap<Long, Double> getWholeGraphPPR() { // return the reference of reserve
		return reserve;
	}
	
	private Double computeEstRandWalkTime(Double rsum_local, Double omega_local) { 
		// compute estimated time (ns) for omega times random walks
		
		return avg_rand_walk_time * rsum_local * omega_local;
	}
	
	@Override
	public void computeWholeGraphPPR(Long nodeId_start, Object epsilon) {
		residue.clear();
		reserve.clear();
		Double rsum_local = rsum, 
				rmax_local = (Double)epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail)) / (1.0 - alpha),
				omega_local = ((Double)epsilon + 2.0) * Math.log(2.0 / pfail) / (Double)epsilon / (Double)epsilon / delta;
		
		long startTime = 0, endTime = 0, duration_fwdpush = 0, duration_random = 0;
        
		// part 1: perform forward push
		Forward_Push fp_section = null;
		while (duration_fwdpush < computeEstRandWalkTime(rsum_local, omega_local)) {
			fp_section = new Forward_Push(alpha, 1.0, node_amount, graphDb, adjM, node_property, dir_db);
		
			startTime = System.nanoTime();
			fp_section.computeWholeGraphPPR(nodeId_start, rmax_local);
			endTime = System.nanoTime();
			duration_fwdpush += (endTime - startTime);
		
			rsum_local = fp_section.getUpdatedRsum() * (1 - alpha);
			rmax_local /= 2.0;
		}

		// performance info:
		//System.out.println("\nFinish forward push in " + duration_fwdpush / 1000000 + "(ms)");
		
		reserve = fp_section.getReserveCopy();
		residue = fp_section.getResidueCopy();
		
		// part 2: perform random walks
		Monte_Carlo mc_section = new Monte_Carlo(alpha, node_amount, graphDb, pfail, delta, 
				adjM, node_property, dir_db);
		
		
		long num_random_walk = (long)(omega_local * rsum_local);
		
		startTime = System.nanoTime();
		for (Map.Entry<Long, Double> entry : residue.entrySet()) { // perform random walk from each v_i for omega_i times
			Long nodeId_cur = entry.getKey();
			Double residue_cur = entry.getValue();
			Double reserve_incr_cur = residue_cur * alpha; // transfer part of residue to reserve
			residue_cur *= (1.0 - alpha); // update residue_cur
			Double old_reserve_cur = reserve.get(nodeId_cur);
			if (old_reserve_cur == null)
				old_reserve_cur = 0.0;
			reserve.put(nodeId_cur, old_reserve_cur + reserve_incr_cur);

			long omega_i = (long)Math.ceil(residue_cur / rsum_local * (double)num_random_walk);
			Double a_i = residue_cur / rsum_local * (double)num_random_walk / (double)omega_i;
			Double reserve_incr = a_i / (double)num_random_walk * rsum_local;

			for (long j = 0; j < omega_i; j++) {
				Long nodeId_dest = mc_section.random_walk_no_zero_hop(nodeId_cur);
				Double old_reserve_dest = reserve.get(nodeId_dest);
				if (old_reserve_dest == null)
					old_reserve_dest = 0.0;
				reserve.put(nodeId_dest, old_reserve_dest + reserve_incr);
			}
		}
		endTime = System.nanoTime();
		duration_random += (endTime - startTime);

		// performance info:
		//System.out.println("\nFinish random walk in " + duration_random / 1000000 + "(ms)");
	}
	
	@Override
	public void preprocessing(Double dummy, Object epsilon) {
		// customize preprocessing_dirName
		preprocessing_dirName += ("/" + epsilon);
		
		System.out.println("\nFORA preprocessing starts...");
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
			computeWholeGraphPPR(nodeId_start, epsilon); // run FORA Whole-Graph SSPPR algo on src node
			
			String fileName_start = preprocessing_dirName + "/" + nodeId_start.toString() + ".txt";
			try { // store ppr values that are not smaller than threshold
				FileWriter fw = new FileWriter(fileName_start, false);
				for (Map.Entry< Long, Double > entry_t : reserve.entrySet()) {
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
		// read pprs from file corresponding to src node and store in reserve
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

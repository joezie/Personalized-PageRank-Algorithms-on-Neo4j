package joezie.fora_neo4j;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

import org.bouncycastle.crypto.tls.TlsAuthentication;
import org.bouncycastle.jcajce.provider.symmetric.ARC4.Base;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

public class Gen_Util { // defines general functions for algo performance tests 
	protected GraphDatabaseService graphDb;
	protected String dir_db;
	protected Graph adjM; // adjacency matrix of the graph
	protected int node_amount;
	protected int rel_amount;
	protected String node_property;
	protected String label_type;
	protected Label node_label;
	protected String rel_type;
	protected Double alpha; // probability that a random walk terminates at a step
	protected Double epsilon; // error bound of estimated and actual pi 
	protected Node nodes[]; // small graph sample
	private String algo_perf_result_file_name;
	private FileWriter fw;

	protected enum RelTypes implements RelationshipType
	{
		LINKS,
		Relation
	}

	protected enum LabelTypes implements Label
	{
		PPR_NODES,
		Person
	}

	protected enum AlgoType {
		MC,
		FWDPUSH,
		FORA_WHOLE_GRAPH,
		FORA_TOPK,
		POWER_METHOD,
		BASE_WHOLE_GRAPH,
		NEO4J_METHOD
	}

	protected enum TestType {
		WHOLE_GRAPH,
		TOPK
	}

	protected enum ErrType {
		PRECISION,
		NDCG,
		MAX_ERR
	}

	protected Object newAlgoObj(AlgoType algo, int k) { // create a new algo object; k is for top-k algp
		Algo_Conf ac_t = new Algo_Conf(alpha, node_property);
		Object ret = null;
		switch (algo) {
		case MC:
			ret = ac_t.set_conf_mc(node_amount, rel_amount, graphDb, adjM, dir_db);
			break;
		case POWER_METHOD:
			ret = ac_t.set_conf_power_method(graphDb, adjM, dir_db);
			break;
		case BASE_WHOLE_GRAPH:
			ret = ac_t.set_conf_base_whole_graph(node_amount, rel_amount, graphDb, adjM, dir_db);
			break;
		case FORA_WHOLE_GRAPH:
			ret = ac_t.set_conf_fora_whole_graph(node_amount, rel_amount, graphDb, adjM, dir_db);
			break;
		case FWDPUSH:
			ret = ac_t.set_conf_fwdpush(node_amount, rel_amount, graphDb, adjM, dir_db);
			break;
		case NEO4J_METHOD:
			ret = ac_t.set_conf_neo4j_method(graphDb, adjM, label_type, rel_type, node_amount, dir_db);
			break;
		case FORA_TOPK:
			ret = ac_t.set_conf_fora_topk(node_amount, rel_amount, k, graphDb, adjM, dir_db);
			break;
		default:
			break;
		}
		if (ret == null)
			System.out.println("Creating a new object of " + algo + " failed!");
		return ret;
	}

	private Vector<Long> getQueryNodes(int query_num) { // randomly pick query_num nodes as query src nodes
		Vector<Long> queryNodes = new Vector<>();

		//queryNodes.add(80759L); // test------------------------------------------

		for (int i = 0; i < query_num; i++) {
			int picked_node_num = ThreadLocalRandom.current().nextInt(node_amount);
			queryNodes.add((long)picked_node_num);
		}
		return queryNodes;
	}

	public void algo_perf_test(AlgoType algoType, int query_num, int k, Object param, Double threshold, 
			Boolean to_be_preprocessed, TestType testType) throws Exception { 
		// test algorithms' performance: executing time and error; k is for top-k algo;
		// param is the parameter used to adjust accuracy of Whole-Graph SSPPR algo; 
		// in the end of preprocessing, we only store ppr values that are not smaller than threshold;
		// to_be_processed is used to indicate whether preprocessing is required or not;

		Vector<Long> queryNodes = getQueryNodes(query_num); // get query src nodes
		HashMap<Integer, Integer> prog_pct_map = new HashMap<>(); // (query_idx, progress percentage)
		for (int i = 10; i < 100; i += 10) {
			int query_idx = query_num * i / 100;
			prog_pct_map.put(query_idx, i);
		}
		prog_pct_map.put(query_num - 1, 100);

		if (testType == TestType.TOPK) { // test top-k algo performance
			Topk_Util_Interface topk_t = (Topk_Util_Interface)newAlgoObj(algoType, k);
			Power_Method pm_t = (Power_Method)newAlgoObj(AlgoType.POWER_METHOD, -1);
			long startTime = 0, endTime = 0, duration = 0, duration_prep = 0;
			Double sum_precision = 0.0, sum_NDCG = 0.0;		

			if (algoType == AlgoType.BASE_WHOLE_GRAPH) { // preprocessing is required for BASE
				startTime = System.nanoTime();
				((Base_Whole_Graph)topk_t).preprocessing(threshold, k); // need sorting
				endTime = System.nanoTime();
				duration_prep += (endTime - startTime);

				System.out.println("\nPreprocessing time for " + algoType + ": " + duration_prep / 1000000 + "(ms)");

				//fw.write("\nthreshold = " + threshold + ", k = " + k
				//		+ "\nPreprocessing time: " + duration_prep / 1000000 + "\n");
				Long prepSize = ((Base_Whole_Graph)topk_t).getPrepSize();
				fw.write(threshold + "," + k + "," + duration_prep / 1000000 + "," + prepSize + ",");
			}
			else { // preprocessing isn't required  
				//fw.write("\nparam = " + param + ", k = " + k);
				fw.write(param + "," + k + ",");
			}

			System.out.println("\nTesting Top-k SSPPR performance of " + algoType + " with " + query_num + " queries");
			for (int i = 0; i < query_num; i++) {
				// run top-k algo
				startTime = System.nanoTime();
				topk_t.computeTopKPPR(queryNodes.get(i), k, param);
				endTime = System.nanoTime();
				duration += (endTime - startTime);

				// run power method
				pm_t.computeTopKPPR(queryNodes.get(i), k, null);

				// compute error
				Double precision = computeError(topk_t, pm_t, ErrType.PRECISION, k);
				Double NDCG = computeError(topk_t, pm_t, ErrType.NDCG, k);
				sum_precision += (precision == null) ? 0.0 : precision;
				sum_NDCG += (NDCG == null) ? 0.0 : NDCG;

				/*
				// debug info
				System.out.println("\n(" + (i + 1) + ")\tsrc node #" + queryNodes.get(i) + "(" + 
						new Algo_Util(node_property).getNodeName(queryNodes.get(i), graphDb) + ")");
				topk_t.printTopKResult(k);
				pm_t.printTopKResult(k);
				System.out.println("\n***precision: " + precision);
				System.out.println("\n***NDCG: " + NDCG);
				*/

				// show progress
				if (prog_pct_map.containsKey(i))
					System.out.println("Progress: " + prog_pct_map.get(i) + "%");
			}
			System.out.println("\nPerformance test of " + algoType + " completed");

			// compute average performance results
			long avg_duration = duration / query_num; // (ns)
			Double avg_precision = sum_precision / query_num;
			Double avg_NDCG = sum_NDCG / query_num;

			System.out.println("\n" + algoType + " performance:"
					+ "\nAverage running time: " + avg_duration / 1000000 + "(ms)"
					+ "\nAverage precision: " + avg_precision
					+ "\nAverage NDCG: " + avg_NDCG
					+ "\n");

			//fw.write("\nAverage running time: " + avg_duration / 1000000
			//		+ "\nAverage precision: " + avg_precision
			//		+ "\nAverage NDCG: " + avg_NDCG + "\n");
			
			fw.write(avg_duration / 1000000 + "," + avg_precision + "," + avg_NDCG + "\n");
		}
		else if (testType == TestType.WHOLE_GRAPH){ // test whole-graph algo performance
			Whole_Graph_Util_Interface algo_t = (Whole_Graph_Util_Interface)newAlgoObj(algoType, -1);
			Preprocessing_Interface prep_algo_t = null;
			Power_Method pm_t = (Power_Method)newAlgoObj(AlgoType.POWER_METHOD, -1);
			long startTime = 0, endTime = 0, duration_compute = 0, duration_prep = 0;
			Double sum_max_err = 0.0;		

			/*
	        // if it's BASE, then we need to preprocess first
	        if (algoType == AlgoType.BASE_WHOLE_GRAPH) {
	        	((Base_Whole_Graph)algo_t).preprocessing();
	        }
			 */


			fw.write(param + ",");
			
			if (to_be_preprocessed || algoType == AlgoType.BASE_WHOLE_GRAPH) { // preprocessing is required
				
				startTime = System.nanoTime();

				prep_algo_t = (Preprocessing_Interface)algo_t;
				prep_algo_t.preprocessing(threshold, param);

				endTime = System.nanoTime();
				duration_prep += (endTime - startTime);

				System.out.println("\nPreprocessing time for " + algoType + ": " + duration_prep / 1000000 + "(ms)");

				//fw.write("\nthreshold = " + threshold.toString() 
				//+ "\nPreprocessing time: " + duration_prep / 1000000 + "\n");
				 
				Long prepSize = prep_algo_t.getPrepSize();
				fw.write(threshold + "," + duration_prep / 1000000 + "," + prepSize + ",");
			}
			
		
			System.out.println("\nTesting performance of " + algoType + " with " + query_num + " queries");
			for (int i = 0; i < query_num; i++) {
				Long nodeId_start = queryNodes.get(i);

				// run algo
				startTime = System.nanoTime();

				if (to_be_preprocessed) // if it's preprocessed, then just read results from file
					prep_algo_t.readPreprocessedPPR(nodeId_start); 
				else // else we need to compute ppr
					algo_t.computeWholeGraphPPR(nodeId_start, param);

				endTime = System.nanoTime();
				duration_compute += (endTime - startTime);

				// run power method
				pm_t.computeWholeGraphPPR(nodeId_start, null);

				// compute error
				Double max_err = computeError(algo_t, pm_t, ErrType.MAX_ERR, -1);
				sum_max_err += max_err;

				/*
		        // debug info
		        System.out.println("\n(" + (i + 1) + ")\tsrc node #" + queryNodes.get(i) + "(" + 
		        		new Algo_Util(node_property).getNodeName(queryNodes.get(i), graphDb) + ")");
		        System.out.println("\n***max_err: " + max_err);
		        algo_t.printResult();
		        pm_t.printResult();
				*/

				// show progress
				if (prog_pct_map.containsKey(i)) 
					System.out.println("Progress: " + prog_pct_map.get(i) + "%");
			}
			System.out.println("\nPerformance test of " + algoType + " completed");

			// compute average performance results
			long avg_duration_compute = duration_compute / query_num; // (ns)
			Double avg_max_err = sum_max_err / query_num;

			System.out.println("\n" + algoType + " performance:"
					+ "\nAverage computing time: " + avg_duration_compute / 1000000 + "(ms)"
					+ "\nAverage max error: " + avg_max_err);

			if (!to_be_preprocessed)
				//fw.write("Avg computing time: " + avg_duration_compute / 1000000 + "\n");
				fw.write(avg_duration_compute / 1000000 + ",");
				
			//fw.write("Average max error: " + avg_max_err + "\n");
			fw.write(avg_max_err + "\n");

			if (to_be_preprocessed && algoType != AlgoType.BASE_WHOLE_GRAPH) 
				// for preprocessing algo other than BASE, we would delete the preprocessing directory
				((Preprocessing_Interface)algo_t).deletePrepDir();
		}
		else {
			System.out.println("Error: Test type should be WHOLE_GRAPH or TOPK!");
			return;
		}
	}

	private Double computeError(Object algo, Power_Method pm, ErrType err_type, int k) { 
		/* Compute error between ground truth and ppr result of each algorithm.
		 * k is for top-k algo 
		 */
		if (err_type == ErrType.PRECISION || err_type == ErrType.NDCG) { // compute error for top-k algo
			//int k = ((Fora_Topk)algo).getK();
			Vector<Long> gnd_topk_nodeIds = pm.getTopKNodeIds(k);
			if (gnd_topk_nodeIds == null) // get ground truth top-k node ids failed
				return null;
			Vector<Long> algo_topk_nodeIds = ((Topk_Util_Interface)algo).getTopKNodeIds(k);
			if (algo_topk_nodeIds == null) // get algo's top-k node ids failed
				return null;

			if (err_type == ErrType.PRECISION) { // precision result
				Double cqt_pred = 0.0; // correct prediction

				for (Long nodeId : algo_topk_nodeIds)
					if (gnd_topk_nodeIds.contains(nodeId)) // increment precision when this estimated top-k node is actually top-k
						cqt_pred++;

				//System.out.println("Correct Predictions:" + cqt_pred + "\nSample Size:" + gnd_topk_nodeIds.size());

				return cqt_pred / (double)gnd_topk_nodeIds.size(); // precision = correct-prediction / size-of-sample; the size of sample is gnd_topk_ppr_res.size(), but not necessarily k 
			}
			else if (err_type == ErrType.NDCG) { // NDCG result
				double Zk = 0.0, DCG = 0.0;

				HashMap<Long, Double> gnd_topk_ppr_res = pm.getTopK(k); // ground truth top-k ppr results computed by power method
				if (gnd_topk_ppr_res == null) // get ground truth top-k results failed
					return null;

				for (int i = 1; i <= gnd_topk_ppr_res.size(); i++) { // Zk = sum_i(2^pi(s,t_i)-1)/log(i+1)/log2
					Double gnd_ppr_i = gnd_topk_ppr_res.get(gnd_topk_nodeIds.get(i - 1));
					Zk += (Math.pow(2.0, gnd_ppr_i) - 1.0) / Math.log(i + 1.0) / Math.log(2.0);
				}

				for (int i = 1; i <= algo_topk_nodeIds.size(); i++) { // DCG = sum_i(2^pi(s,t_i')-1)/log(i+1)/log2
					Double algo_ppr_i = gnd_topk_ppr_res.get(algo_topk_nodeIds.get(i - 1));
					if (algo_ppr_i == null)
						algo_ppr_i = 0.0;
					DCG += (Math.pow(2.0, algo_ppr_i) - 1.0) / Math.log(i + 1.0) / Math.log(2.0);
				}

				//System.out.println("DCG:" + DCG + "\nZk:" + Zk);

				return DCG / Zk; // NDCG = DCG / Zk
			}
			else {
				System.out.println("computeError's argument err_type should be PRECISION or NDCG");
				return null;
			}
		}
		else if (err_type == ErrType.MAX_ERR){ // compute the max error of |pi(s, v) - pi'(s, v)| among all nodes v
			Double maxErr = 0.0;
			HashMap<Long, Double> gnd_ppr_res = pm.getWholeGraphPPR(); // ground truth ppr results computed by power method
			HashMap<Long, Double> algo_ppr_res = ((Whole_Graph_Util_Interface)algo).getWholeGraphPPR(); // estimated ppr results computed by algo

			for (Map.Entry<Long, Double> gnd_entry : gnd_ppr_res.entrySet()) {
				Long nodeId = gnd_entry.getKey();
				Double real_ppr = gnd_entry.getValue(); // ground ppr value computed by power method
				Double est_ppr = algo_ppr_res.get(nodeId); // estimated ppr value computed by algo
				if (est_ppr == null)
					est_ppr = 0.0;
				Double err = Math.abs(est_ppr - real_ppr);
				maxErr = Math.max(maxErr, err);
			}
			return maxErr;
		}
		else {
			System.out.println("computeError's argument err_type should be PRECISION, NDCG or MAX_ERR");
			return null;
		}
	}

	public void algo_perf_batch_test(int query_num, int k) throws Exception { 
		algo_perf_result_file_name = dir_db + "_AlgoPerfResults.txt";
		/* perform algo tests of all kinds in batches
		 * k for top-k algo
		 */

		// parameters for BlogCatalog
		/*
		// parameters for whole-graph
		Double threshold_arr_base_whole_graph[] = {0.001, 7.0E-4, 5.0E-4, 1.0E-4, 5.0E-5}; // for BASE
		Double epsilon_arr_fora_whole_graph[] = {50.0, 10.0, 5.0, 1.0, 0.5}; // for FORA-Whole-Graph
		Double epsilon_arr_mc_whole_graph[] = {5.0, 1.0, 0.7, 0.5, 0.3}; // for Monte-Carlo
		Double rmax_arr_whole_graph[] = {1.0E-6, 7.0E-7, 5.0E-7, 3.0E-7, 1.0E-7}; // for Forward Push
		Integer iteration_arr_whole_graph[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_whole_graph[] = {-1}; // for BASE
		
		
		// parameters for top-k
		Double threshold_arr_base_topk[] = {0.001, 7.0E-4, 5.0E-4, 1.0E-4, 5.0E-5}; // for BASE
		Double epsilon_arr_fora_topk[] = {1.0, 0.5, 0.1, 0.05, 0.01}; // for FORA-Topk
		Double epsilon_arr_mc_topk[] = {5.0, 1.0, 0.5, 0.1, 0.05}; // for Monte-Carlo
		Double rmax_arr_topk[] = {1.0E-6, 5.0E-7, 1.0E-7, 5.0E-8, 1.0E-8}; // for Forward Push
		Integer iteration_arr_topk[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_topk[] = {-1}; // for BASE


		// parameters for preprocessing
		Double threshold_arr_base_prep[] = {0.001, 7.0E-4, 5.0E-4, 1.0E-4, 5.0E-5}; // for BASE
		Double threshold_arr_other_prep[] = {-1.0}; // for other preprocessing algo
		Double epsilon_arr_fora_prep[] = {50.0, 10.0, 5.0, 1.0, 0.5}; // for FORA-Whole-Graph
		Double epsilon_arr_mc_prep[] = {5.0, 1.0, 0.7, 0.5, 0.3}; // for Monte-Carlo
		Double rmax_arr_prep[] = {1.0E-6, 7.0E-7, 5.0E-7, 3.0E-7, 1.0E-7}; // for Forward Push
		Integer iteration_arr_prep[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_prep[] = {-1}; // for BASE
		*/
		
		// parameters for Flickr
		/*
		// parameters for preprocessing
		Double epsilon_arr_fora_prep[] = {100.0, 70.0}; // for FORA-Whole-Graph
		Double threshold_arr_base_prep[] = {0.01, 0.005, 0.001, 7.0E-4, 5.0E-4, 1.0E-4, 7.0E-5, 5.0E-5, 3.0E-5, 1.0E-5}; // for BASE
		Double epsilon_arr_mc_prep[] = {20.0}; // for Monte-Carlo
		Double rmax_arr_prep[] = {}; // for Forward Push
		Integer iteration_arr_prep[] = {}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_prep[] = {-1}; // for BASE
		Double threshold_arr_other_prep[] = {-1.0}; // for other preprocessing algo
		*/
		// test param arrays for preprocessing
		/* Double threshold_arr_base_prep[] = {0.01, 0.005, 0.001, 7.0E-4, 5.0E-4, 1.0E-4, 7.0E-5, 5.0E-5, 3.0E-5, 1.0E-5}; // for BASE
		 * Double epsilon_arr_fora_prep[] = {500.0, 200.0, 100.0, 70.0, 50.0}; // for FORA-Whole-Graph
		 * Double rmax_arr_prep[] = {5.0E-5, 1.0E-5, 5.0E-6, 1.0E-6, 7.0E-7}; // for Forward Push
		 * Integer iteration_arr_prep[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		 * Double epsilon_arr_mc_prep[] = {20.0, 10.0, 7.0, 5.0, 3.0}; // for Monte-Carlo
		 */
		
		
		// real param arrays for whole-graph
		/* Double epsilon_arr_fora_whole_graph[] = {500.0, 200.0, 50.0, 20.0, 5.0}; // for FORA-Whole-Graph
		 * Double rmax_arr_whole_graph[] = {5.0E-5, 1.0E-6, 5.0E-8, 1.0E-8, 5.0E-9}; // for Forward Push
		 * Integer iteration_arr_whole_graph[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		 * Double epsilon_arr_mc_whole_graph[] = {10.0, 5.0, 1.0, 0.5, 0.3}; // for Monte-Carlo
		 *
		 */
		
		// parameters for grqc
		
		// parameters for top-k
		/*
		Double threshold_arr_base_topk[] = {0.001, 5.0E-4, 5.0E-5, 1.0E-7, 5.0E-8}; // for BASE
		Double epsilon_arr_fora_topk[] = {}; // for FORA-Whole-Graph
		Double epsilon_arr_mc_topk[] = {}; // for Monte-Carlo
		Double rmax_arr_topk[] = {}; // for Forward Push
		Integer iteration_arr_topk[] = {}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_topk[] = {-1}; // for BASE
		*/
		// real param array for top-k
		/* Double threshold_arr_base_topk[] = {0.001, 5.0E-4, 5.0E-5, 1.0E-7, 5.0E-8}; // for BASE
		 * Double epsilon_arr_fora_topk[] = {10.0, 0.5, 0.1, 0.01, 0.001}; // for FORA-Whole-Graph
		 * Double epsilon_arr_mc_topk[] = {3.0, 1.0, 0.2, 0.1, 0.05}; // for Monte-Carlo
		 * Double rmax_arr_topk[] = {1.0E-4, 1.0E-6, 5.0E-8, 7.0E-9, 7.0E-10}; // for Forward Push
		 * Integer iteration_arr_topk[] = {5, 40, 300, 500, 1000}; // for Neo4j Method
		 * Integer dummy_whole_graph_k_arr_topk[] = {-1}; // for BASE
		 */		
		
		// real param array for whole-graph
		/* Double epsilon_arr_fora_whole_graph[] = {10.0, 5.0, 0.5, 0.1, 0.05}; // for FORA-Whole-Graph
		 * Double rmax_arr_whole_graph[] = {1.0E-4, 1.0E-5, 1.0E-6, 1.0E-7, 1.0E-8}; // for Forward Push
		 * Integer iteration_arr_whole_graph[] = {5, 40, 100, 200, 300}; // for Neo4j Method
		 * Double epsilon_arr_mc_whole_graph[] = {1.0, 0.5, 0.3, 0.1, 0.05}; // for Monte-Carlo
		 * Double threshold_arr_base_whole_graph[] = {0.001, 5.0E-4, 5.0E-5, 1.0E-6, 5.0E-7}; // for BASE
		 * Integer dummy_whole_graph_k_arr_whole_graph[] = {-1}; // for BASE
		 */
		
		// real param array for preprocessing
		/* Double epsilon_arr_fora_whole_graph[] = {10.0, 5.0, 0.5, 0.3, 0.1}; // for FORA-Whole-Graph
		 * Double rmax_arr_whole_graph[] = {1.0E-4, 1.0E-5, 1.0E-6, 5.0E-7, 1.0E-7}; // for Forward Push
		 * Integer iteration_arr_whole_graph[] = {5, 40, 100, 200, 300}; // for Neo4j Method
		 * Double epsilon_arr_mc_whole_graph[] = {1.0, 0.5, 0.3, 0.2, 0.1}; // for Monte-Carlo
		 * Double threshold_arr_base_whole_graph[] = {0.001, 5.0E-4, 5.0E-5, 1.0E-6, 5.0E-7}; // for BASE
		 */
		
		// parameters for amazon
		
		// parameters for whole-graph
		
		Double threshold_arr_base_whole_graph[] = {4.0E-5, 3.0E-5, 2.0E-5, 1.0E-5}; // for BASE
		Double epsilon_arr_fora_whole_graph[] = {}; // for FORA-Whole-Graph
		Double epsilon_arr_mc_whole_graph[] = {}; // for Monte-Carlo
		Double rmax_arr_whole_graph[] = {}; // for Forward Push
		Integer iteration_arr_whole_graph[] = {}; // for Neo4j Method
		Integer dummy_whole_graph_k_arr_whole_graph[] = {-1}; // for BASE
		
		
		// test param array for whole-graph
		/* Double threshold_arr_base_whole_graph[] = {5.0E-5, 4.0E-5, 3.0E-5, 2.0E-5, 1.0E-5}; // for BASE
		 * Double epsilon_arr_fora_whole_graph[] = {50.0, 10.0, 5.0, 3.0, 1.0}; // for FORA-Whole-Graph
		 * Double epsilon_arr_mc_whole_graph[] = {1.0, 0.7, 0.5, 0.3, 0.1}; // for Monte-Carlo
		 * Double rmax_arr_whole_graph[] = {1.0E-6, 5.0E-7, 3.0E-7, 5.0E-8, 1.0E-8}; // for Forward Push
		 * Integer iteration_arr_whole_graph[] = {1, 5, 10, 40, 100}; // for Neo4j Method
		 * Integer dummy_whole_graph_k_arr_whole_graph[] = {-1}; // for BASE
		 */
		
		
		// algo classifications
		AlgoType preprocessingAlgos[] = {
				AlgoType.FORA_WHOLE_GRAPH, 
				AlgoType.FWDPUSH, 
				AlgoType.NEO4J_METHOD,
				AlgoType.MC, 
				AlgoType.BASE_WHOLE_GRAPH
		};
		AlgoType wholeGraphAlgos[] = preprocessingAlgos;
		AlgoType topKAlgos[] = {
				AlgoType.FORA_TOPK, 
				AlgoType.FWDPUSH, 
				AlgoType.NEO4J_METHOD,
				AlgoType.MC, 
				AlgoType.BASE_WHOLE_GRAPH
		};

		// 1. open AlgoPerfResults.txt to store performance results
		fw = new FileWriter(algo_perf_result_file_name, true);

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		fw.write(sdf.format(new Date()) + "\n"); // write current time

		fw.close();

		// 2. tests

		// Test 1. Whole-Graph test

		fw = new FileWriter(algo_perf_result_file_name, true);
		fw.write("\nTest 1. Whole-Graph test\n");
		fw.close();
		for (Integer i = 0; i < wholeGraphAlgos.length; i++) {
			AlgoType algoType = wholeGraphAlgos[i];
			Object param_arr[] = null;
			if (algoType == AlgoType.MC)
				param_arr = epsilon_arr_mc_whole_graph;
			else if (algoType == AlgoType.FORA_WHOLE_GRAPH)
				param_arr = epsilon_arr_fora_whole_graph;
			else if (algoType == AlgoType.FWDPUSH)
				param_arr = rmax_arr_whole_graph;
			else if (algoType == AlgoType.NEO4J_METHOD)
				param_arr = iteration_arr_whole_graph;
			else if (algoType == AlgoType.BASE_WHOLE_GRAPH)
				param_arr = dummy_whole_graph_k_arr_whole_graph;
			else {
				System.out.println("Invalid algo type!");
				return;
			}

			Integer idx = i + 1;
			fw = new FileWriter(algo_perf_result_file_name, true);
			fw.write("\n1." + idx + " " + algoType + "\n");
			fw.close();
			if (algoType == AlgoType.BASE_WHOLE_GRAPH) {

				for (Double thres : threshold_arr_base_whole_graph) {
					fw = new FileWriter(algo_perf_result_file_name, true);
					algo_perf_test(algoType, query_num, -1, -1, thres, false, TestType.WHOLE_GRAPH);
					fw.close();
				}
			}
			else {

				for (Object param : param_arr) {
					fw = new FileWriter(algo_perf_result_file_name, true);
					algo_perf_test(algoType, query_num, -1, param, -1.0, false, TestType.WHOLE_GRAPH);
					fw.close();
				}
			}
		}

/*
		// Test 2. Top-k test
		fw = new FileWriter(algo_perf_result_file_name, true);
		fw.write("\nTest 2. Top-k test\n");
		fw.close();
		for (Integer i = 0; i < topKAlgos.length; i++) {
			AlgoType algoType = topKAlgos[i];
			Object param_arr[] = null;
			if (algoType == AlgoType.MC)
				param_arr = epsilon_arr_mc_topk;
			else if (algoType == AlgoType.FORA_TOPK)
				param_arr = epsilon_arr_fora_topk;
			else if (algoType == AlgoType.FWDPUSH)
				param_arr = rmax_arr_topk;
			else if (algoType == AlgoType.NEO4J_METHOD)
				param_arr = iteration_arr_topk;
			else if (algoType == AlgoType.BASE_WHOLE_GRAPH)
				param_arr = dummy_whole_graph_k_arr_topk;
			else {
				System.out.println("Invalid algo type!");
				return;
			}

			Integer idx = i + 1;
			fw = new FileWriter(algo_perf_result_file_name, true);
			fw.write("\n2." + idx + " " + algoType + "\n");
			fw.close();
			if (algoType == AlgoType.BASE_WHOLE_GRAPH) {
				for (Double thres : threshold_arr_base_topk) {
					fw = new FileWriter(algo_perf_result_file_name, true);
					algo_perf_test(algoType, query_num, k, -1, thres, false, TestType.TOPK);
					fw.close();
				}
			}
			else {
				for (Object param : param_arr) {
					fw = new FileWriter(algo_perf_result_file_name, true);
					algo_perf_test(algoType, query_num, k, param, -1.0, false, TestType.TOPK);
					fw.close();
				}
			}
		}
*/
/*
		// Test 3. Preprocessing test
		fw = new FileWriter(algo_perf_result_file_name, true);
		fw.write("\nTest 3. Preprocessing test\n");
		fw.close();
		for (Integer i = 0; i < preprocessingAlgos.length; i++) {
			AlgoType algoType = preprocessingAlgos[i];
			Object param_arr[] = null;
			if (algoType == AlgoType.MC)
				param_arr = epsilon_arr_mc_prep;
			else if (algoType == AlgoType.FORA_WHOLE_GRAPH)
				param_arr = epsilon_arr_fora_prep;
			else if (algoType == AlgoType.FWDPUSH)
				param_arr = rmax_arr_prep;
			else if (algoType == AlgoType.NEO4J_METHOD)
				param_arr = iteration_arr_prep;
			else if (algoType == AlgoType.BASE_WHOLE_GRAPH)
				param_arr = dummy_whole_graph_k_arr_prep;
			else {
				System.out.println("Invalid algo type!");
				return;
			}

			Integer idx = i + 1;
			fw = new FileWriter(algo_perf_result_file_name, true);
			fw.write("\n3." + idx + " " + algoType + "\n");
			fw.close();

			if (algoType == AlgoType.BASE_WHOLE_GRAPH) {
				for (Double thres : threshold_arr_base_prep) {
					fw = new FileWriter(algo_perf_result_file_name, true);
					algo_perf_test(algoType, query_num, -1, -1, thres, true, TestType.WHOLE_GRAPH);
					fw.close();
				}
			}
			else { // for other preprocessing algo
				for (Object param : param_arr) {
					for (Double thres : threshold_arr_other_prep) {
						fw = new FileWriter(algo_perf_result_file_name, true);
						algo_perf_test(algoType, query_num, -1, param, thres, true, TestType.WHOLE_GRAPH);
						fw.close();
					}
				}
			}
		}
*/

		// 3. close AlgoPerfResults.txt
		//fw.close();

	}
}

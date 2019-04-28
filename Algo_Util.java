package joezie.fora_neo4j;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class Algo_Util { // functions used by each algorithm
	protected String node_property;
	//protected HashMap<Node, Vector<Node>> extracted_graph; // (node, in/out neighbors)
	
	public Algo_Util(String node_property) {
		this.node_property = node_property;
	}
	
	protected String getNodeName(Long nodeId, GraphDatabaseService graphDb) {
		
		String nodeName = "";
		try ( Transaction tx = graphDb.beginTx() ) {
			Node node = graphDb.getNodeById(nodeId);
			nodeName = node.getProperty(node_property).toString();	
			tx.success();		
		}
		return nodeName;
	}
	
	protected Double kth_ppr(Object pprs[], int k) {
		/* Return the kth largest ppr value.
		 */
		
		return find_kth_element(pprs, 0, pprs.length - 1, k);
	}
	
	private Double find_kth_element(Object arr[], int left, int right, int k) {
		/* Return the kth largest element in the array bounded by index
		 * left and right.
		 */
		
		if (k >= 0 && k <= right - left + 1) {
			int pos = random_partition(arr, left, right), offset = pos - left;
			if (offset == k - 1)
				return (Double)arr[pos];
			else if (offset > k - 1)
				return find_kth_element(arr, left, pos - 1, k);
			return find_kth_element(arr, pos + 1, right, k - pos + left - 1);
		}
		return null; // return null when k is invalid
	}
	
	private int random_partition(Object arr[], int left, int right) { 
		/* Return the index of the randomly selected pivot after partition.
		 * After partition, the elements before pivot would be greater,
		 * while the element after it would be smaller.
		 */
		
		int n = right - left + 1, i = left;
		int pivot_index = ThreadLocalRandom.current().nextInt(n);
		swap(arr, left + pivot_index, right); // randomly select a pivot and exchange it with the rightmost element
		Object pivot = arr[right];
		for (int j = left; j <= right - 1; j++) {
			if (((Comparable)arr[j]).compareTo(pivot) > 0) {
				swap(arr, i, j);
				i++;
			}
		}
		swap(arr, i, right);
		return i;
	}
	
	private void swap(Object arr[], int i1, int i2) {
		Object tmp = arr[i1];
		arr[i1] = arr[i2];
		arr[i2] = tmp;
	}

	
	/*
	protected void extract_graph(GraphDatabaseService graphDb, Direction dir) { 
		// extract graph of certain direction and store in extracted_graph
		
		extracted_graph = new HashMap<>();
		try (Transaction tx = graphDb.beginTx()) {
			for (Node node : graphDb.getAllNodes()) {
				Vector<Node> dir_neighbors = new Vector<>();
				for (Relationship rel : node.getRelationships(dir)) 
					dir_neighbors.add(rel.getOtherNode(node));
				extracted_graph.put(node, dir_neighbors);
			}
			tx.success();
		}
	}
	*/
	/*
	public static void main( String[] args ) throws IOException {
		Double arr[] = {1.0, 3.0, 4.0, 2.0, 5.0};
		Fora_Util fu_t = new Fora_Util();
		for (int k = 0; k < arr.length; k++) {
			Double kth_element = fu_t.kth_ppr(arr, k);
			System.out.println(k + "th element:" + kth_element);
		}
	}
	*/
}

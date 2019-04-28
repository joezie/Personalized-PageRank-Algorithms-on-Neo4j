package joezie.fora_neo4j;

import org.bouncycastle.jcajce.provider.symmetric.ARC4.Base;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.GraphDatabaseService;

public class Algo_Conf { // configuration setting for each algorithm
	private String node_property;
	// tag::configuration parameters[]
    private Double alpha; // probability that a random walk terminates at a step
    //private Double epsilon; // error bound of estimated and actual pi 
    private Double delta; // threshold of pi that we care
    private Double pfail; // failure probability that the error bound can't be satisfied
    //private Double rmax; // residue threshold of local update
    private Double rsum; // sum of all nodes' residues
    // end::configuration parameters[]
    
    // tag::configuration parameters for fora-topk[]
    private int k;
    private Double min_delta;
    private Double min_rmax;
    // end::configuration parameters for fora-topk[]
    
    public Algo_Conf(Double alpha, String node_property) {
    	this.alpha = alpha;
    	//this.epsilon = epsilon;
    	this.node_property = node_property;
    }

    public Power_Method set_conf_power_method(GraphDatabaseService graphDb, Graph adjM, String dir_db) {
    	return new Power_Method(graphDb, alpha, adjM, node_property, dir_db);
    }
    
    public Monte_Carlo set_conf_mc(int node_amount, int rel_amount, GraphDatabaseService graphDb, 
    		Graph adjM, String dir_db) {
    	//alpha = 0.15;
        //epsilon = 0.5;
        delta = 1.0 / (double)node_amount;
        pfail = 1.0 / (double)node_amount;
        //rmax = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
        rsum = 1.0;
        return new Monte_Carlo(alpha, node_amount, graphDb, pfail, delta, adjM, node_property, dir_db);
    }
    
    public Base_Whole_Graph set_conf_base_whole_graph(int node_amount, int rel_amount, 
    		GraphDatabaseService graphDb, Graph adjM, String dir_db) {
    	//alpha = 0.15;
        //epsilon = 0.5;
        delta = 1.0 / (double)node_amount;
        pfail = 1.0 / (double)node_amount;
        //rmax = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
        //rmax = 0.001;
        
        return new Base_Whole_Graph(alpha, node_amount, graphDb, adjM, node_property, dir_db);
    }
    
    public Fora_Whole_Graph set_conf_fora_whole_graph(int node_amount, int rel_amount, 
    		GraphDatabaseService graphDb, Graph adjM, String dir_db) {
    	//alpha = 0.15;
        //epsilon = 0.5;
        delta = 1.0 / (double)node_amount;
        pfail = 1.0 / (double)node_amount;
        //rmax = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
        rsum = 1.0;
        
        // tag::new version[]
        //rmax *= 1.0 / (1.0 - alpha);
        // end::new version[]
        
        return new Fora_Whole_Graph(alpha, rsum, pfail, delta, node_amount, rel_amount, graphDb, 
        		adjM, node_property, dir_db);
    }
    
    
    public Forward_Push set_conf_fwdpush(int node_amount, int rel_amount, GraphDatabaseService graphDb, 
    		Graph adjM, String dir_db) {
    	//alpha = 0.15;
        //epsilon = 0.5;
        delta = 1.0 / (double)node_amount;
        pfail = 1.0 / (double)node_amount;
        //rmax = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
        rsum = 1.0;
        return new Forward_Push(alpha, rsum, node_amount, graphDb, adjM, node_property, dir_db);
    }
    
    public Neo4j_Method set_conf_neo4j_method(GraphDatabaseService graphDb, Graph adjM, String label_type,
    		String rel_type, int node_amount, String dir_db) {
    	return new Neo4j_Method(graphDb, alpha, label_type, rel_type, adjM, node_amount, node_property
    			, dir_db);
    }
    
    public Fora_Topk set_conf_fora_topk(int node_amount, int rel_amount, int k, 
    		GraphDatabaseService graphDb, Graph adjM, String dir_db) {
        min_delta = 1.0 / (double)node_amount; // min delta: 1/n
        
        // tag::new version[]
        this.k = k;
        delta = 1.0 / (double)k; // initial delta: 1/k
        pfail = 1.0 / (double)node_amount / (double)node_amount / Math.log(node_amount / k); // pfail' = pfail / n
        //rmax = epsilon * Math.sqrt(delta / 3.0 / (double)rel_amount / Math.log(2.0 / pfail));
        //rmax *= Math.sqrt((double)rel_amount * rmax) * 3.0;
        //epsilon *= 0.5; //epsilon' = epsilon / 2
        // end::new version[]

        //min_rmax = epsilon * Math.sqrt(min_delta / 3 / rel_amount / Math.log(2 / pfail));
        rsum = 1.0;
        
        return new Fora_Topk(alpha, rsum, pfail, delta, node_amount, rel_amount, graphDb, 
        		min_delta, k, adjM, node_property, dir_db);
    }
    
}

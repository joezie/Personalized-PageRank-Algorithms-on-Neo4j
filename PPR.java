package joezie.fora_neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.bouncycastle.crypto.modes.KCCMBlockCipher;
import org.neo4j.cypher.internal.compiler.v2_3.ast.QueryTagger.fromString;
import org.neo4j.cypher.internal.compiler.v2_3.helpers.StringRenderingSupport;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Nodes;
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.printCommandsToString;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.internal.GraphDatabaseAPI;


public class PPR extends Gen_Util { // main class
	//private static final File databaseDirectory = new File( "target/undirected_graph.db" );
	//private static final File databaseDirectory = new File( "target/directed_graph.db" ); // directed graph with 80792 Nodes and 1713090 Relationships
	//private static final File databaseDirectory = new File( "target/small_graph_sample.db" );
	//private static final File databaseDirectory = new File( "target/small_graph_sample_reverse.db" );
	//private static final File databaseDirectory = new File( "target/small_graph_sample_undirected.db" );
	//private static final File databaseDirectory = new File( "target/5_nodes_graph.db" );
	
	/*
	public PPR(Double alpha, Double epsilon) {
		super(alpha, epsilon);
	}
	*/
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                System.out.println( "\nShutting down database ..." );
                graphDb.shutdown();
            }
        } );
    }
	
	public void createDb(File databaseDirectory) throws IOException
    {
		System.out.println( "\nCreating or opening database " + databaseDirectory.getName() + "...");

		super.dir_db = databaseDirectory.getName();
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( databaseDirectory );
        registerShutdownHook( graphDb );
		System.out.println( "Database created or opened" );
    }
	
	public void shutDown()
    {
        System.out.println( "\nShutting down database ..." );
        // tag::shutdownServer[]
        graphDb.shutdown();
        // end::shutdownServer[]
    }
	
	public void construct_5_nodes_graph() {
		try ( Transaction tx = graphDb.beginTx() )
        {
			node_amount = 5;
			nodes = new Node[node_amount];
			for (int i = 0; i < node_amount; i++) {
				nodes[i] = graphDb.createNode(LabelTypes.PPR_NODES);
				nodes[i].setProperty("name", i);
			}
				
			nodes[0].createRelationshipTo(nodes[1],RelTypes.LINKS);
			nodes[0].createRelationshipTo(nodes[2],RelTypes.LINKS);
			nodes[1].createRelationshipTo(nodes[3],RelTypes.LINKS);
			nodes[2].createRelationshipTo(nodes[4],RelTypes.LINKS);
			
            tx.success();
        }
	}
	
	public void construct_graph() {
		try ( Transaction tx = graphDb.beginTx() )
        {
			node_amount = 7;
			nodes = new Node[node_amount];
			for (int i = 0; i < node_amount; i++) {
				nodes[i] = graphDb.createNode(LabelTypes.PPR_NODES);
				nodes[i].setProperty("name", i);
			}
				
			nodes[0].createRelationshipTo(nodes[1],RelTypes.LINKS);
			nodes[1].createRelationshipTo(nodes[2],RelTypes.LINKS);
			nodes[1].createRelationshipTo(nodes[3],RelTypes.LINKS);
			nodes[2].createRelationshipTo(nodes[4],RelTypes.LINKS);
			nodes[2].createRelationshipTo(nodes[5],RelTypes.LINKS);
			nodes[3].createRelationshipTo(nodes[5],RelTypes.LINKS);
			nodes[3].createRelationshipTo(nodes[6],RelTypes.LINKS);
			nodes[5].createRelationshipTo(nodes[4],RelTypes.LINKS);
			nodes[5].createRelationshipTo(nodes[6],RelTypes.LINKS);
			
            tx.success();
        }
	}
	
	public void construct_graph_reverse() {
		try ( Transaction tx = graphDb.beginTx() )
        {
			node_amount = 7;
			nodes = new Node[node_amount];
			for (int i = 0; i < node_amount; i++) {
				nodes[i] = graphDb.createNode(LabelTypes.PPR_NODES);
				nodes[i].setProperty("name", i);
			}
				
			nodes[1].createRelationshipTo(nodes[0],RelTypes.LINKS);
			nodes[2].createRelationshipTo(nodes[1],RelTypes.LINKS);
			nodes[3].createRelationshipTo(nodes[1],RelTypes.LINKS);
			nodes[4].createRelationshipTo(nodes[2],RelTypes.LINKS);
			nodes[5].createRelationshipTo(nodes[2],RelTypes.LINKS);
			nodes[5].createRelationshipTo(nodes[3],RelTypes.LINKS);
			nodes[6].createRelationshipTo(nodes[3],RelTypes.LINKS);
			nodes[4].createRelationshipTo(nodes[5],RelTypes.LINKS);
			nodes[6].createRelationshipTo(nodes[5],RelTypes.LINKS);
			
            tx.success();
        }
	}
	
	public void turn_undir_to_dir_graph() {
		try ( Transaction tx = graphDb.beginTx() )
        {
			Iterator<Relationship> iter = graphDb.getAllRelationships().iterator();
			while (iter.hasNext()) {
				Relationship rel = iter.next();
				rel.getEndNode().createRelationshipTo(rel.getStartNode(), RelTypes.LINKS);
			}
			tx.success();
        }
	}
	
	
	public void create_index() {
		System.out.println( "\nCreating index ..." );
		
		// tag::createIndex[]
        IndexDefinition indexDefinition;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            
            // check whether we have created this index or not
            for (IndexDefinition idxdef : schema.getIndexes(node_label)) {
            	for (String pkey : idxdef.getPropertyKeys()) {
            		if (pkey.equals(node_property)) {
            			System.out.println("Already created index " + node_label.name() + "("
            					+ node_property + ")");
            			tx.success();
            			return;
            		}
            	}
            }
            
            // if not created yet, then create this index
            indexDefinition = schema.indexFor( node_label )
                    .on( node_property )
                    .create();
            tx.success();
        }
        // end::createIndex[]
        
        // tag::wait[]
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
        }
        // end::wait[]
        // tag::progress[]
        float completed_percentage = 0;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            completed_percentage = schema.getIndexPopulationProgress( indexDefinition ).getCompletedPercentage();
        	System.out.println();
            System.out.println( String.format( "Percent complete: %1.0f%%", completed_percentage));
        }
        if (completed_percentage == 100) {
        	System.out.println();
    		System.out.println( "Index created" );
        }
        else {
        	System.out.println();
    		System.out.println( "Index creation failed" );
        }
        // end::progress[]
	}
	
	public void set_configuration(Double alpha, Double epsilon, String node_property, String label_type, 
			String rel_type) {
	    this.alpha = alpha;
	    this.epsilon = epsilon;
		this.node_property = node_property;
		this.label_type = label_type;
		this.rel_type = rel_type;
		node_label = Label.label(label_type);
		node_amount = (int)(adjM.nodeCount());
		try ( Transaction tx = graphDb.beginTx() ) {
			rel_amount = (int)Iterators.count(graphDb.getAllRelationships().iterator());
			tx.success();
		}
		/*
		node_property = "name";
		label_type = "Person";
		node_label = Label.label("Person");
		rel_type = "Relation";
		*/
	}
	
	public void setupAdjMatrix() {
		System.out.println("\nLoading graph from database...");
		long startTime = 0, endTime = 0, duration = 0;
		startTime = System.nanoTime();
		
		adjM = new GraphLoader((GraphDatabaseAPI)graphDb)
				.withoutRelationshipWeights()
				.withoutNodeWeights()
				.withoutNodeProperties()
				.withAnyRelationshipType()
				.withAnyLabel()
				.load(HeavyGraphFactory.class);
		
		endTime = System.nanoTime();
		duration = endTime - startTime;
		System.out.println("\nFinish graph loading in " + duration / 1000000 + "(ms)");
	}

	
	public Node test_get_node_id(long nodeId) {
		Node node0 = null;
		try ( Transaction tx = graphDb.beginTx() )
        {
			node0 = graphDb.getNodeById(nodeId);
			tx.success();
        }
		if (node0 == null)
			System.out.println("Cannot find node0");
		return node0;
	}
	
	public int test_get_node_number() {
		int node_num = 0;
		try ( Transaction tx = graphDb.beginTx() )
        {
			node_num = (int)Iterators.count(graphDb.findNodes(LabelTypes.Person));
			tx.success();
        }
		return node_num;
	}
	/*
	public void test_iterable(Node node0) {
		Iterable<Relationship> iter = null;
		try ( Transaction tx = graphDb.beginTx() )
        {
			iter = node0.getRelationships(Direction.OUTGOING);
			tx.success();
        }
		Iterator<Relationship> it = iter.iterator();
	}
	*/
	
	
    public static void main( String[] args ) throws IOException
    {
    	//PPR ppr_t = new PPR(0.15, 0.5); // alpha = 0.15; epsilon = 0.5
    	int k, query_num;
    	Double alpha, epsilon, rmax;
    	String node_property = null, label_type = null, rel_type = null, dir_db = null, dir_base_prep = null;
    	AlgoType algo = null;
    	
    	// default value
    	alpha = 0.15;
    	epsilon = 0.5;
    	rmax = 0.001;
    	k = 50;
    	query_num = 50;
    	node_property = "name";
		label_type = "Person";
		rel_type = "Relation";
		dir_base_prep = "BASE_ppr_results";
		
		// values that must be inputed by user
		algo = AlgoType.FORA_WHOLE_GRAPH;
		
		//dir_db = "target/directed_graph.db";
		//dir_db = "target/undirected_graph.db";
		//dir_db = "target/small_graph_sample.db";
		//dir_db = "target/small_graph_sample_undirected.db";
		//dir_db = "target/blog_catalog.db";
		//dir_db = "target/flickr.db";
		//dir_db = "target/grqc.db";
		dir_db = "target/amazon.db";
		//dir_db = "target/got.db";
		
    	/* Read arguments from command line */
		/*
		System.out.println("\nUsage: ./PPR database_dir <algo> [options]"
				+ "\n<algo>:"
				+ "\n1. MC\t\t\tTest Monte-Carlo Algorithm"
				+ "\n2. FWP\t\t\tTest Forward Push Algorithm"
				+ "\n3. BASE\t\t\tTest Base Whole-Graph Algorithm"
				+ "\n4. FORA_WG\t\t\tTest Fora Whole-Graph Algorithm"
				+ "\n5. FORA_TOPK\t\t\tTest Fora Top-K Algorithm"
				+ "\n<options>:"
				+ "\n1. --alpha\t\t\tThe possibility that a random walk stops at current node"
				+ "\n2. --epsilon\t\t\tThe relative error bound"
				+ "\n3. --query\t\t\tThe number of queries for the test"
				+ "\n4. --k\t\t\tFor Top-k Algorithm tests: the number of nodes with greatest PPR value that we're interested in"
				+ "\n5. --rmax\t\t\tFor BASE Algorithm tests: the absolute error bound"
				+ "\n6. --node_property\t\t\tThe node property in the input database"
				+ "\n7. --label_type\t\t\tThe nodes' label type in the input datatbase"
				+ "\n8. --rel_type\t\t\tThe relationships' type in the input database"
				+ "\n9. --base_dir\t\t\tThe directory where the results of BASE preprocessing would store"
				);
		
		int argc = args.length;  
		if (argc < 3) {
			System.out.println("Error: too few command-line arguments!");
			return;
		}
		dir_db = args[1];
		*/
    	/* Read arguments from command line */
		
    	
    	PPR ppr_t = new PPR();
        ppr_t.createDb(new File(dir_db));
        
        //ppr_t.construct_5_nodes_graph(); // ----------------test------------------
        //ppr_t.construct_graph(); // ----------------test------------------
        //ppr_t.construct_graph_reverse(); // ----------------test------------------
        //ppr_t.turn_undir_to_dir_graph(); // ----------------test------------------
        
        ppr_t.setupAdjMatrix();
        ppr_t.set_configuration(alpha, epsilon, node_property, label_type, rel_type);
        ppr_t.create_index();
        
        //Node node0 = ppr_t.test_get_node_xidada();
        //Node src_small_sample = ppr_t.test_get_node_id(2); // ----------------test------------------
        long startTime = 0L, endTime = 0L, duration = 0L;
        
        
        // 1. Monte-Carlo
        /*
        ppr_t.set_configuration(AlgoType.MC);
        Monte_Carlo mc_t = new Monte_Carlo(ppr_t.alpha, ppr_t.epsilon, ppr_t.node_amount, ppr_t.graphDb,
        		ppr_t.pfail, ppr_t.delta, ppr_t.adjM);
        System.out.println("\nWhole-Graph SSPPR query using Monte-Carlo Algo starts...");
		startTime = System.nanoTime();
        mc_t.monte_carlo(node0.getId());
		//mc_t.monte_carlo(src_small_sample.getId());
		endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish Monte Carlo in " + duration / 1000000 + "(ms)");
        
        mc_t.printResult();
        */
        /*
        Monte_Carlo mc_t = (Monte_Carlo)ppr_t.newAlgoObj(AlgoType.MC, -1);
        System.out.println("\nWhole-Graph SSPPR query using Monte-Carlo Algo starts...");
		startTime = System.nanoTime();
        //mc_t.computePPR(node0.getId());
		mc_t.computePPR(src_small_sample.getId());
		endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish Monte Carlo in " + duration / 1000000 + "(ms)");
        
        mc_t.printResult();
        */
        
        // 2. Forward-Push
        /*
        ppr_t.set_configuration(AlgoType.FWDPUSH);
        System.out.println("\nrmax: " + ppr_t.rmax);
        Forward_Push fp_t = new Forward_Push(ppr_t.alpha, ppr_t.rmax, ppr_t.rsum, ppr_t.node_amount, 
        		ppr_t.graphDb, ppr_t.adjM);
       
        System.out.println("\nWhole-Graph SSPPR query using Forward Push Algo starts...");
        startTime = System.nanoTime();
        fp_t.forward_push_whole_graph(node0.getId());
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish forward Push in " + duration / 1000000 + "(ms)");
        
        //fp_t.printResult();
        */
        
        // 3. Fora-Whole-Graph
        /*
        Algo_Conf ac_t = new Algo_Conf(ppr_t.alpha, ppr_t.node_property);
        Fora_Whole_Graph fwg_t = ac_t.set_conf_fora_whole_graph(ppr_t.node_amount, ppr_t.rel_amount, ppr_t.graphDb, ppr_t.adjM, ppr_t.dir_db);
        System.out.println("\nWhole-Graph SSPPR query using FORA Algo starts...");
        startTime = System.nanoTime();
        fwg_t.computeWholeGraphPPR(31L, ppr_t.epsilon);
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish FORA-Whole-Graph-SSPPR in " + duration / 1000000 + "(ms)");
        
        
        fwg_t.printWholeGraphResult();
        */
        
        // 4. Fora-TopK
        /*
        Algo_Conf ac_t = new Algo_Conf(ppr_t.alpha, ppr_t.node_property);
        Fora_Topk ftk_t = ac_t.set_conf_fora_topk(ppr_t.node_amount, ppr_t.rel_amount, 10, ppr_t.graphDb, ppr_t.adjM, ppr_t.dir_db);
        System.out.println("\nTop-k SSPPR query using FORA Algo starts...");
        startTime = System.nanoTime();
        ftk_t.computeTopKPPR(31L, -1, ppr_t.epsilon);
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish FORA-Top-k-SSPPR in " + duration / 1000000 + "(ms)");
        
        ftk_t.printTopKResult(-1);
        */
        
        // 5. BASE
        /*
        ppr_t.set_configuration(AlgoType.BASE_WHOLE_GRAPH);
        System.out.println("\nrmax: " + ppr_t.rmax);
        Base_Whole_Graph bwg_t = new Base_Whole_Graph(ppr_t.alpha, ppr_t.rmax, ppr_t.node_amount, ppr_t.graphDb, ppr_t.adjM);
        bwg_t.preprocessing();
        System.out.println("\nWhole-Graph SSPPR query using BASE Algo starts...");
        startTime = System.nanoTime();
        bwg_t.base_whole_graph(node0.getId());
        //bwg_t.base_whole_graph(src_small_sample.getId()); // ----------------test------------------
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish BASE-Whole-Graph-SSPPR in " + duration / 1000000 + "(ms)");
        
        //bwg_t.printResult();
        */
        
        // 6. Neo4j PPR
        /*        
        ppr_t.set_configuration(AlgoType.NEO4J_METHOD);
        Neo4j_Method neo_t = new Neo4j_Method(ppr_t.graphDb, ppr_t.alpha, ppr_t.label_type, ppr_t.rel_type, ppr_t.adjM, ppr_t.node_amount);
        System.out.println("\nWhole-Graph SSPPR query using Neo4j Algo starts...");
        startTime = System.nanoTime();
        //neo_t.computePPR(node0.getId());
        neo_t.computePPR(src_small_sample.getId());
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish Neo4j SSPPR in " + duration / 1000000 + "(ms)");
        
        neo_t.printResult();
        */
        
        // 7. Power Method
        /*
        //ppr_t.set_configuration(AlgoType.POWER_METHOD);
        //System.out.println("rmax: " + ppr_t.rmax);
        //Power_Method pm_t = new Power_Method(ppr_t.graphDb, ppr_t.alpha, ppr_t.adjM);
        Power_Method pm_t = (Power_Method)ppr_t.newAlgoObj(AlgoType.POWER_METHOD, -1);
        System.out.println("\nWhole-Graph SSPPR query using Power Method Algo starts...");
        startTime = System.nanoTime();
        pm_t.computePPR(node0.getId());
        //pm_t.compute_exact_ppr(src_small_sample.getId()); // ----------------test------------------
        endTime = System.nanoTime();
        duration = (endTime - startTime);
        System.out.println("\nFinish Power Method in " + duration / 1000000 + "(ms)");
        
        pm_t.printTopKResult(50);
        */
        
        /*
        try ( Transaction tx = ppr_t.graphDb.beginTx() ) {
			int node_amount = (int)Iterators.count(ppr_t.graphDb.getAllNodes().iterator());
			int rel_amount = (int)Iterators.count(ppr_t.graphDb.getAllRelationships().iterator());
			System.out.println("Node Number:" + node_amount + "\tRel Number" + rel_amount);
			tx.success();
        }
        */
        //System.out.println("Xidada's node id:" + node0.getId());
        
        /*
        Double MaxErr = ppr_t.computeError(bwg_t, pm_t, -1);
        System.out.println("\nMaxErr:" + MaxErr);
        */
        
        /*
        Double precision = ppr_t.computeError(ftk_t, pm_t, 0);
        System.out.println("Precision:" + precision);
        Double NDCG = ppr_t.computeError(ftk_t, pm_t, 1);
        System.out.println("NDCG:" + NDCG);
        */
        
        
        try {
        	ppr_t.algo_perf_batch_test(20, 50);
        	//ppr_t.algo_perf_test(AlgoType.FORA_WHOLE_GRAPH, 5, 50, 500.0, -1.0, false, TestType.WHOLE_GRAPH);
        }
        catch (Exception e) {
			System.out.println("Algo performance batch test failed!");
			e.printStackTrace();
		}
		
		
        
        /*
        for (long i = 0; i < ppr_t.node_amount; i++) {
        	int nodeIdM_cur = ppr_t.adjM.toMappedNodeId(i); // current node id in adjacency matrix
			int out_degree_cur = ppr_t.adjM.degree(nodeIdM_cur, Direction.OUTGOING);
			if (out_degree_cur == 0)
				System.out.println("#" + i);
        }
        */
        
        ppr_t.shutDown();
    }
}

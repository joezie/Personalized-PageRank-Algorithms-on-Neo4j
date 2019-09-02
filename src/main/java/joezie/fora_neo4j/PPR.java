package joezie.fora_neo4j;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;


public class PPR extends Gen_Util { // main class

	private static final String ALPHA_OPTION = "alpha";
	private static final String EPSILON_OPTION = "epsilon";
	private static final String QUERY_OPTION = "query";
	private static final String K_OPTION = "k";
	private static final String NODE_PROPERTY_OPTION = "node_property";
	private static final String LABEL_TYPE_OPTION = "label_type";
	private static final String REL_TYPE_OPTION = "rel_type";
	private static final String DATABASE_DIR_OPTION = "db_dir";
	private static final String HELP_OPTION = "help";
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application)
		
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
	
	public void create_index() {
		System.out.println( "\nCreating index ..." );
		
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
        
        try ( Transaction tx = graphDb.beginTx() )
        {
            Schema schema = graphDb.schema();
            schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
        }
        
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

    public static void main( String[] args ) throws IOException
    {
    	// set command line options
    	Options cliOptions = new Options();
        cliOptions.addOption("alpha", ALPHA_OPTION, true, "The possibility that a random walk stops at current node (Default: 0.15)");
        cliOptions.addOption("eps", EPSILON_OPTION, true, "The relative error bound (Default: 0.5)");
        cliOptions.addOption("query", QUERY_OPTION, true, "The number of queries for the test (Default: 50)");
        cliOptions.addOption("k", K_OPTION, true, "For Top-k Algorithm tests: the number of nodes with greatest PPR value that we're interested in (Default: 10)");
        cliOptions.addOption("node", NODE_PROPERTY_OPTION, true, "The node property in the input database (Default: \"name\")");
        cliOptions.addOption("label", LABEL_TYPE_OPTION, true, "The nodes' label type in the input datatbase (Default: \"Person\")");
        cliOptions.addOption("rel", REL_TYPE_OPTION, true, "The relationships' type in the input database (Default: \"Relation\")");
        cliOptions.addOption("db", DATABASE_DIR_OPTION, true, "The directory of the input database (Default: \"target/got.db\")");
        cliOptions.addOption("help", HELP_OPTION, false, "Print information about command line inputs.");
        
        try {
            // Read and validate command line arguments
            CommandLine line = new DefaultParser().parse(cliOptions, args);
            
            if (line.hasOption(HELP_OPTION)) {
                new HelpFormatter().printHelp("PPR", cliOptions);
                return;
            }

            final Double alpha = Double.parseDouble(line.getOptionValue(ALPHA_OPTION, "0.15"));
            final Double epsilon = Double.parseDouble(line.getOptionValue(EPSILON_OPTION, "0.5"));
            final int query_num = Integer.parseInt(line.getOptionValue(QUERY_OPTION, "50"));
            final int k = Integer.parseInt(line.getOptionValue(K_OPTION, "10"));
            final String node_property = line.getOptionValue(NODE_PROPERTY_OPTION, "name");
            final String label_type = line.getOptionValue(LABEL_TYPE_OPTION, "Person");
            final String rel_type = line.getOptionValue(REL_TYPE_OPTION, "Relation");
            final String dir_db = line.getOptionValue(DATABASE_DIR_OPTION, "target/got.db");
     
            PPR ppr_t = new PPR();
            ppr_t.createDb(new File(dir_db));
            ppr_t.setupAdjMatrix();
            ppr_t.set_configuration(alpha, epsilon, node_property, label_type, rel_type);
            ppr_t.create_index();

        	ppr_t.algo_perf_batch_test(query_num, k);

        	ppr_t.shutDown();
        }
        catch (Exception e) {
			System.out.println("Algo performance batch test failed!");
			e.printStackTrace();
		}
    }
}

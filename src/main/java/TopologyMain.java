import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;
import spouts.WordReader;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 2)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //LocalCluster cluster = new LocalCluster(); //cluster.submitTopology("Count-Word-Topology-With-Refresh-Cache", conf,builder.createTopology());
        StormSubmitter.submitTopology("Count-Word-Topology", conf,
                builder.createTopology()); //Thread.sleep(1000); //cluster.shutdown();
    }
}

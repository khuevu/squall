package ch.epfl.data.plan_runner.storm_components.hyper_cube;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.TupleStorage;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by khayyam on 3/20/15.
 */

public class StormHyperCubeJoin extends StormBoltComponent {

    public StormHyperCubeJoin (ArrayList<StormEmitter> emitters, ComponentProperties cp,
                               List<String> allCompNames, Predicate joinPredicate, int hierarchyPosition,
                               TopologyBuilder builder, TopologyKiller killer, Config conf,
                               InterchangingComponent interComp, TypeConversion wrapper) {

        super(cp, allCompNames, hierarchyPosition, false, conf);
    }
    @Override
    public void aggBatchSend() {
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
                                         List<String> tuple, long lineageTimestamp, boolean isLastInBatch) {
    }

    private void createIndexes() {

    }

    @Override
    public void execute(Tuple stormTupleRcv) {

    }

    @Override
    public ChainOperator getChainOperator() {
        return null;
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return getConf();
    }

    @Override
    public String getInfoID() {
        final String str = "DestinationStorage " + getID() + " has ID: "
                + getID();
        return str;
    }

    @Override
    protected InterchangingComponent getInterComp() {
        return null;
    }

    @Override
    public long getNumSentTuples() {
        return 0;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
        return null;
    }

    private void join(Tuple stormTuple, List<String> tuple,
                      boolean isFromFirstEmitter, TupleStorage oppositeStorage,
                      boolean isLastInBatch) {
    }

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
                               String inputTupleHash, boolean isFromFirstEmitter,
                               List<Index> oppositeIndexes, List<String> valuesToApplyOnIndex,
                               TupleStorage oppositeStorage, boolean isLastInBatch) {
        final TupleStorage tuplesToJoin = new TupleStorage();
        selectTupleToJoin(oppositeStorage, oppositeIndexes, isFromFirstEmitter,
                valuesToApplyOnIndex, tuplesToJoin);
        join(stormTupleRcv, tuple, isFromFirstEmitter, tuplesToJoin,
                isLastInBatch);
    }

    @Override
    protected void printStatistics(int type) {

    }

    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     String inputTupleHash, Tuple stormTupleRcv, boolean isLastInBatch) {
    }

    private void selectTupleToJoin(TupleStorage oppositeStorage,
                                   List<Index> oppositeIndexes, boolean isFromFirstEmitter,
                                   List<String> valuesToApplyOnIndex, TupleStorage tuplesToJoin) {

    }

    private List<String> updateIndexes(String inputComponentIndex,
                                       List<String> tuple, List<Index> affectedIndexes, int row_id) {
        return null;
    }
}

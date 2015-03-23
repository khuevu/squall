package ch.epfl.data.plan_runner.storm_components.hyper_cube;

import backtype.storm.Config;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.TupleStorage;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.ContentSensitiveMatrixAssignment;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.EquiMatrixAssignment;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.MatrixAssignment;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Created by khayyam on 3/20/15.
 */

public class StormHyperCubeJoin extends StormBoltComponent {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormHyperCubeJoin.class);
    private List<TupleStorage> relationStorages;
    private List<String> emitterIndexes;
    private long numSentTuples = 0;
    private List<Predicate> joinPredicates;

    private ChainOperator operatorChain;
    // position to test for equality in first and second emitter
    // join params of current storage then other relation interchangably !!
    List<Integer> _joinParams;
    private List<List<Index>> relationIndexes;
    private List<Integer> _operatorForIndexes;
    private List<Object> _typeOfValueIndexed;
    private boolean existIndexes = false;
    // for agg batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private long _aggBatchOutputMillis;
    private InterchangingComponent _inter = null;

    // for printing statistics for creating graphs
    protected Calendar _cal = Calendar.getInstance();
    protected DateFormat _dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected SimpleDateFormat _format = new SimpleDateFormat(
            "EEE MMM d HH:mm:ss zzz yyyy");
    protected StatisticsUtilities _statsUtils;

    public StormHyperCubeJoin (ArrayList<StormEmitter> emitters, ComponentProperties cp,
                               List<String> allCompNames, List<Predicate> joinPredicates, int hierarchyPosition,
                               TopologyBuilder builder, TopologyKiller killer, Config conf,
                               InterchangingComponent interComp, TypeConversion wrapper) {

        super(cp, allCompNames, hierarchyPosition, false, conf);

        emitterIndexes = new ArrayList<String>();
        for (int i = 0; i < emitters.size(); i++) {
            emitterIndexes.add(String.valueOf(allCompNames.indexOf(emitters.get(i)
                    .getName())));
        }

        _aggBatchOutputMillis = cp.getBatchOutputMillis();
        _statsUtils = new StatisticsUtilities(getConf(), LOG);
        final int parallelism = SystemParameters.getInt(conf, getID() + "_PAR");
        operatorChain = cp.getChainOperator();
        this.joinPredicates = joinPredicates;
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

        /*************** Should be finished *****************/
        // Change to HuperCube implementation
        final MatrixAssignment _currentMappingAssignment;

        _currentMappingAssignment = new ContentSensitiveMatrixAssignment(conf);

        if (interComp == null)
            currentBolt = MyUtilities.hyperCubeAttachEmitterComponents(currentBolt,
                    emitters, allCompNames,
                    _currentMappingAssignment, conf, wrapper);
        else {
            currentBolt = MyUtilities
                    .hypecCubeAttachEmitterComponentsWithInterChanging(currentBolt,
                            emitters, allCompNames,
                            _currentMappingAssignment, conf, interComp);
            _inter = interComp;
        }


        if (getHierarchyPosition() == FINAL_COMPONENT && (!MyUtilities.isAckEveryTuple(conf)))
            killer.registerComponent(this, parallelism);

        if (cp.getPrintOut() && operatorChain.isBlocking())
            currentBolt.allGrouping(killer.getID(), SystemParameters.DUMP_RESULTS_STREAM);

        relationStorages = new ArrayList<TupleStorage>();
        for (int i = 0; i < emitters.size(); i++)
            relationStorages.add(new TupleStorage());


        if (joinPredicates != null && joinPredicates.size() > 0) {
            createIndexes();
            existIndexes = true;
        } else
            existIndexes = false;

    }
    @Override
    public void aggBatchSend() {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            if (operatorChain != null) {
                final Operator lastOperator = operatorChain.getLastOperator();
                if (lastOperator instanceof AggregateOperator) {
                    try {
                        _semAgg.acquire();
                    } catch (final InterruptedException ex) {
                    }
                    // sending
                    final AggregateOperator agg = (AggregateOperator) lastOperator;
                    final List<String> tuples = agg.getContent();
                    for (final String tuple : tuples)
                        tupleSend(MyUtilities.stringToTuple(tuple, getConf()),
                                null, 0);
                    // clearing
                    agg.clearStorage();
                    _semAgg.release();
                }
            }

    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
                                         List<String> tuple, long lineageTimestamp, boolean isLastInBatch) {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            try {
                _semAgg.acquire();
            } catch (final InterruptedException ex) {
            }
        tuple = operatorChain.process(tuple);
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            _semAgg.release();
        if (tuple == null)
            return;
        numSentTuples++;
        printTuple(tuple);
        if (numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
            printStatistics(SystemParameters.OUTPUT_PRINT);
        if (MyUtilities
                .isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
            long timestamp = 0;
            if (MyUtilities.isCustomTimestampMode(getConf()))
                if (getHierarchyPosition() == StormComponent.NEXT_TO_LAST_COMPONENT)
                    // A tuple has a non-null timestamp only if the component is
                    // next to last because we measure the latency of the last
                    // operator
                    timestamp = System.currentTimeMillis();
            // timestamp = System.nanoTime();
            tupleSend(tuple, stormTupleRcv, timestamp);
        }
        if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf()))
            printTupleLatency(numSentTuples - 1, lineageTimestamp);

    }


    /*************** Should be finished *****************/
    private void createIndexes() {

    }

    @Override
    public void execute(Tuple stormTupleRcv) {
        if (_firstTime
                && MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis)) {
            _periodicAggBatch = new PeriodicAggBatchSend(_aggBatchOutputMillis,
                    this);
            _firstTime = false;
        }

        if (receivedDumpSignal(stormTupleRcv)) {
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }

        if (!MyUtilities.isManualBatchingMode(getConf())) {
            final String inputComponentIndex = stormTupleRcv
                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final List<String> tuple = (List<String>) stormTupleRcv
                    .getValueByField(StormComponent.TUPLE); // getValue(1);
            final String inputTupleHash = stormTupleRcv
                    .getStringByField(StormComponent.HASH);// getString(2);
            if (processFinalAck(tuple, stormTupleRcv))
                return;
            final String inputTupleString = MyUtilities.tupleToString(tuple,
                    getConf());
            processNonLastTuple(inputComponentIndex, inputTupleString, tuple,
                    inputTupleHash, stormTupleRcv, true);
        } else {
            final String inputComponentIndex = stormTupleRcv
                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final String inputBatch = stormTupleRcv
                    .getStringByField(StormComponent.TUPLE);// getString(1);
            final String[] wholeTuples = inputBatch
                    .split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
            final int batchSize = wholeTuples.length;
            for (int i = 0; i < batchSize; i++) {
                // parsing
                final String currentTuple = new String(wholeTuples[i]);
                final String[] parts = currentTuple
                        .split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
                String inputTupleHash = null;
                String inputTupleString = null;
                if (parts.length == 1)
                    // lastAck
                    inputTupleString = new String(parts[0]);
                else {
                    inputTupleHash = new String(parts[0]);
                    inputTupleString = new String(parts[1]);
                }
                final List<String> tuple = MyUtilities.stringToTuple(
                        inputTupleString, getConf());
                // final Ack check
                if (processFinalAck(tuple, stormTupleRcv)) {
                    if (i != batchSize - 1)
                        throw new RuntimeException(
                                "Should not be here. LAST_ACK is not the last tuple!");
                    return;
                }
                // processing a tuple
                if (i == batchSize - 1)
                    processNonLastTuple(inputComponentIndex, inputTupleString,
                            tuple, inputTupleHash, stormTupleRcv, true);
                else
                    processNonLastTuple(inputComponentIndex, inputTupleString,
                            tuple, inputTupleHash, stormTupleRcv, false);
            }
        }
        getCollector().ack(stormTupleRcv);
    }

    @Override
    public ChainOperator getChainOperator() {
        return operatorChain;
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
        return _inter;
    }

    @Override
    public long getNumSentTuples() {
        return numSentTuples;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
        return _periodicAggBatch;
    }

    /*************** Should be finished *****************/
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

    /*************** Should be finished *****************/
    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     String inputTupleHash, Tuple stormTupleRcv, boolean isLastInBatch) {
    }

    /*************** Should be finished *****************/
    private void selectTupleToJoin(TupleStorage oppositeStorage,
                                   List<Index> oppositeIndexes, boolean isFromFirstEmitter,
                                   List<String> valuesToApplyOnIndex, TupleStorage tuplesToJoin) {

    }

    /*************** Should be finished *****************/
    private List<String> updateIndexes(String inputComponentIndex,
                                       List<String> tuple, List<Index> affectedIndexes, int row_id) {
        return null;
    }
}

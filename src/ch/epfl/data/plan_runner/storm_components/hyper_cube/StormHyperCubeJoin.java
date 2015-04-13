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
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storage.TupleStorage;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.StormEmitter;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.thetajoin.indexes.Index;
import ch.epfl.data.plan_runner.thetajoin.matrix_mapping.*;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import ch.epfl.data.plan_runner.visitors.PredicateCreateIndexesVisitor;
import ch.epfl.data.plan_runner.visitors.PredicateUpdateIndexesVisitor;
import gnu.trove.list.array.TIntArrayList;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
    private Map<String, Predicate> joinPredicates;

    private ChainOperator operatorChain;
    // position to test for equality in first and second emitter
    // join params of current storage then other relation interchangably !!
    private Map<String, List<Index>> firstRelationIndexes;
    private Map<String, List<Index>> secondRelationIndexes;

    private Map<String, List<Integer>> operatorForIndexes;
    private Map<String, List<Object>> typeOfValueIndexed;
    Map<String, List<String>> valuesToIndexMap = new HashMap<String, List<String>>();
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
                               List<String> allCompNames, Map<String, Predicate> joinPredicates, int hierarchyPosition,
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
        final int parallelism = 64;//SystemParameters.getInt(conf, getID() + "_PAR");
        operatorChain = cp.getChainOperator();
        this.joinPredicates = joinPredicates;
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);

        final HyperCubeAssignment _currentMappingAssignment;
        long[] cardinality = new long[emitters.size()];
        for (int i = 0; i < emitters.size(); i++)
            cardinality[i] = SystemParameters.getInt(conf, emitters.get(i).getName() + "_CARD");
        _currentMappingAssignment = new HyperCubeAssignerFactory().getAssigner(64, cardinality);

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


        if (joinPredicates != null) {
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

    private void createIndexes() {
        for (int i = 0; i < emitterIndexes.size(); i++) {
          for (int j = i + 1; j < emitterIndexes.size(); j++) {
              String key = emitterIndexes.get(i) + emitterIndexes.get(j);
              String keyReverse = emitterIndexes.get(j) + emitterIndexes.get(i);
              if (joinPredicates.containsKey(key)) {
                  Predicate pr = joinPredicates.get(key);
                  final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
                  pr.accept(visitor);
                  firstRelationIndexes.put(key, new ArrayList<Index>(visitor._firstRelationIndexes));

                  secondRelationIndexes.put(keyReverse, new ArrayList<Index>(visitor._secondRelationIndexes));

                  operatorForIndexes.put(key, new ArrayList<Integer>(visitor._operatorForIndexes));
                  typeOfValueIndexed.put(key, new ArrayList<Object>(visitor._typeOfValueIndexed));
              }
          }
        }
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
            processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
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
                    processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, true);
                else
                    processNonLastTuple(inputComponentIndex, inputTupleString, tuple, stormTupleRcv, false);
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

    public void joinThereComponents(List<TupleStorage> relations, List<List<String>> outputTuples) {
        for (int i = 0; i < relations.get(0).size(); i++) {
            String tuple1String = relations.get(0).get(i);
            final List<String> typl1Tuple = MyUtilities.stringToTuple(tuple1String, getComponentConfiguration());

            for (int j = 0; j < relations.get(1).size(); j++) {
                String tuple2String = relations.get(1).get(i);
                final List<String> typl2Tuple = MyUtilities.stringToTuple(tuple2String, getComponentConfiguration());

                for (int k = 0; k < relations.get(2).size(); k++) {
                    String tuple3String = relations.get(2).get(i);
                    final List<String> typl3Tuple = MyUtilities.stringToTuple(tuple3String, getComponentConfiguration());

                    List<String> outputTuple = null;
                    //outputTuple = MyUtilities.createOutputTupleForThere(typl1Tuple, typl2Tuple, typl3Tuple);
                    outputTuples.add(outputTuple);
                }
            }
        }
    }

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple, int rowID,
                               String emitterIndex, boolean isLastInBatch) {
        /*
        * form of join -->     A -- B -- C -- D -- E
        *
        * Consider we get tuple from C.
        *
        * First we look to the lef and collect tuple in A -- B -- C
        * Then for each A -- B -- C we collect tuples in D -- E
        * */

        int index = -1;
        for (int i = 0; i < emitterIndexes.size(); i++) {
            if (emitterIndex.equals(emitterIndexes.get(i))) {
                index = i;
                break;
            }
        }


        ArrayList<Integer> result = new ArrayList<Integer>();
        List<List<String>> outputTuples = new ArrayList<List<String>>();
        result.add(rowID);

        if (index == 0) { // leftmost relation
            goRight(1, result, outputTuples);
        } else if (index == emitterIndexes.size() - 1) { // rightmost relation
            goLeft(index - 1, result, outputTuples);
        } else { // in the middle
            List<List<String>> leftOutputTuples = new ArrayList<List<String>>();
            goLeft(index - 1, result, leftOutputTuples);

            List<List<String>> rightOutputTuples = new ArrayList<List<String>>();
            goRight(index + 1, result, rightOutputTuples);

            for (int i = 0; i < leftOutputTuples.size(); i++) {
                for (int j = 0; j < rightOutputTuples.size(); j++) {
                    List<String> leftPart = leftOutputTuples.get(i);
                    List<String> rightPart = rightOutputTuples.get(j);

                    // [A - B - C]  [C - D - E] so
                    // we should remove one of the 'C'
                    leftPart.remove(leftPart.size() - 1);

                    leftPart.addAll(rightPart);

                    outputTuples.add(leftPart);
                }
            }

        }

        long lineageTimestamp = 0;
        for (List<String> tpl : outputTuples) {
            applyOperatorsAndSend(stormTupleRcv, tpl,
                    lineageTimestamp, isLastInBatch);
        }

      }


    public void goRight(int relationIndex, ArrayList<Integer> joinResult, List<List<String>> outputTuples) {

        // we have rowA, rowB, rowC, rowD, rowE => we can create tuple
        if (relationIndex == relationStorages.size() - 1) {
            List<List<String>> tuple = new ArrayList<List<String>>();
            for (int i = 0; i < joinResult.size(); i++) {

                String oppositeTupleString = relationStorages.get(i).get(joinResult.get(i));
                final List<String> oppositeTuple = MyUtilities.stringToTuple(
                        oppositeTupleString, getComponentConfiguration());
                tuple.add(oppositeTuple);
                List<String> outputTuple = MyUtilities.createOutputTuple(tuple);
                outputTuples.add(outputTuple);
            }
        }

        int rowId = joinResult.get(joinResult.size());
        LinkedList<Integer> tuplesToJoin = new LinkedList<Integer>();
        selectTupleToJoinFromRight(relationIndex, tuplesToJoin);

        for (int id : tuplesToJoin) {
            ArrayList<Integer> newJoinResult = (ArrayList<Integer>)joinResult.clone();
            newJoinResult.add(id);
            goRight(relationIndex + 1, newJoinResult, outputTuples);
        }
    }

    public void goLeft(int relationIndex, ArrayList<Integer> joinResult, List<List<String>> outputTuples) {
        // we have rowA, rowB, rowC, rowD, rowE => we can create tuple
        if (relationIndex == 0) {
            List<List<String>> tuple = new ArrayList<List<String>>();
            for (int i = 0; i < joinResult.size(); i++) {

                String oppositeTupleString = relationStorages.get(i).get(joinResult.get(i));
                final List<String> oppositeTuple = MyUtilities.stringToTuple(
                        oppositeTupleString, getComponentConfiguration());
                tuple.add(oppositeTuple);
                List<String> outputTuple = MyUtilities.createOutputTuple(tuple);
                outputTuples.add(outputTuple);
            }
        }

        int rowId = joinResult.get(0);
        LinkedList<Integer> tuplesToJoin = new LinkedList<Integer>();
        selectTupleToJoinFromLeft(relationIndex, tuplesToJoin);

        for (int id : tuplesToJoin) {
            ArrayList<Integer> newJoinResult = (ArrayList<Integer>)joinResult.clone();
            newJoinResult.add(0, id);
            goRight(relationIndex - 1, newJoinResult, outputTuples);
        }
    }

    private void selectTupleToJoinFromLeft(int relationIndex, List<Integer> tuplesToJoin) {
        boolean isFromFirstEmitter = false;
        String key = emitterIndexes.get(relationIndex - 1) + emitterIndexes.get(relationIndex);
        String keyReverse = emitterIndexes.get(relationIndex) + emitterIndexes.get(relationIndex - 1);
        List<Index> oppositeIndexes = firstRelationIndexes.get(key);

        Predicate pr = joinPredicates.get(key);
        final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
        pr.accept(visitor);

        selectTupleToJoin(key, oppositeIndexes, isFromFirstEmitter, tuplesToJoin);
    }



    private void selectTupleToJoinFromRight(int relationIndex, List<Integer> tuplesToJoin) {
        boolean isFromFirstEmitter = true;
        String key = emitterIndexes.get(relationIndex) + emitterIndexes.get(relationIndex + 1);
        String keyReverse = emitterIndexes.get(relationIndex + 1) + emitterIndexes.get(relationIndex);
        List<Index> oppositeIndexes = firstRelationIndexes.get(keyReverse);

        Predicate pr = joinPredicates.get(key);
        final PredicateCreateIndexesVisitor visitor = new PredicateCreateIndexesVisitor();
        pr.accept(visitor);

        selectTupleToJoin(key, oppositeIndexes, isFromFirstEmitter, tuplesToJoin);
    }


    private void selectTupleToJoin(String key, List<Index> oppositeIndexes, boolean isFromFirstEmitter,
                                   List<Integer> tuplesToJoin) {


        final TIntArrayList rowIds = new TIntArrayList();
        // If there is atleast one index (so we have single join conditions with
        // 1 index per condition)
        // Get the row indices in the storage of the opposite relation that
        // satisfy each join condition (equijoin / inequality)
        // Then take the intersection of the returned row indices since each
        // join condition
        // is separated by AND

        for (int i = 0; i < oppositeIndexes.size(); i++) {
            TIntArrayList currentRowIds = null;

            final Index currentOpposIndex = oppositeIndexes.get(i);
            final String value = valuesToIndexMap.get(key).get(i);

            int currentOperator = operatorForIndexes.get(key).get(i);
            // Switch inequality operator if the tuple coming is from the other
            // relation
            if (isFromFirstEmitter) {
                final int operator = currentOperator;

                if (operator == ComparisonPredicate.GREATER_OP)
                    currentOperator = ComparisonPredicate.LESS_OP;
                else if (operator == ComparisonPredicate.NONGREATER_OP)
                    currentOperator = ComparisonPredicate.NONLESS_OP;
                else if (operator == ComparisonPredicate.LESS_OP)
                    currentOperator = ComparisonPredicate.GREATER_OP;
                else if (operator == ComparisonPredicate.NONLESS_OP)
                    currentOperator = ComparisonPredicate.NONGREATER_OP;
                else
                    currentOperator = operator;
            }

            // Get the values from the index (check type first)
            if (typeOfValueIndexed.get(key).get(i) instanceof String)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        value);
                // Even if valueIndexed is at first time an integer with
                // precomputation a*col +b, it become a double
            else if (typeOfValueIndexed.get(key).get(i) instanceof Double)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Double.parseDouble(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Integer)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Integer.parseInt(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Long)
                currentRowIds = currentOpposIndex.getValues(currentOperator,
                        Long.parseLong(value));
            else if (typeOfValueIndexed.get(key).get(i) instanceof Date)
                try {
                    currentRowIds = currentOpposIndex.getValues(
                            currentOperator, _format.parse(value));
                } catch (final ParseException e) {
                    e.printStackTrace();
                }
            else
                throw new RuntimeException("non supported type");
            // Compute the intersection
            // TODO: Search only within the ids that are in rowIds from previous
            // conditions If
            // nothing returned (and since we want intersection), no need to
            // proceed.
            if (currentRowIds == null)
                return;
            // If it's the first index, add everything. Else keep the
            // intersection
            if (i == 0)
                rowIds.addAll(currentRowIds);
            else
                rowIds.retainAll(currentRowIds);
            // If empty after intersection, return
            if (rowIds.isEmpty())
                return;
        }
        // generate tuplestorage
        for (int i = 0; i < rowIds.size(); i++) {
            final int id = rowIds.get(i);
            tuplesToJoin.add(id);
        }
    }



    @Override
    protected void printStatistics(int type) {
    }


    private void processNonLastTuple(String inputComponentIndex,
                                     String inputTupleString, //
                                     List<String> tuple, // these two are the same
                                     Tuple stormTupleRcv, boolean isLastInBatch) {
        TupleStorage affectedStorage = null;

        for (int i = 0; i < emitterIndexes.size(); i++) {
            if (inputComponentIndex.equals(emitterIndexes.get(i))) {
                affectedStorage = relationStorages.get(i);
                break;
            }
        }

        // add the stormTuple to the specific storage
        if (MyUtilities.isStoreTimestamp(getConf(), getHierarchyPosition())) {
            final long incomingTimestamp = stormTupleRcv
                    .getLongByField(StormComponent.TIMESTAMP);
            inputTupleString = incomingTimestamp
                    + SystemParameters.STORE_TIMESTAMP_DELIMITER
                    + inputTupleString;
        }

        final int row_id = affectedStorage.insert(inputTupleString);
        if (existIndexes)
            updateIndexes(inputComponentIndex, tuple, row_id);

        performJoin(stormTupleRcv, tuple, row_id, inputComponentIndex, isLastInBatch);

    }


    private void updateIndexes(String inputComponentIndex, List<String> tuple, int row_id) {

        for (int i = 0; i < emitterIndexes.size(); i++) {
            String key = inputComponentIndex + emitterIndexes.get(i);
            String keyReverse = emitterIndexes.get(i) + inputComponentIndex;

            if (firstRelationIndexes.containsKey(key)) {
                List<Index> affectedIndexes = firstRelationIndexes.get(key);
                final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(true, tuple);
                Predicate _joinPredicate = joinPredicates.get(key);
                _joinPredicate.accept(visitor);

                final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
                        visitor._typesOfValuesToIndex);
                final List<String> valuesToIndex = new ArrayList<String>(
                        visitor._valuesToIndex);

                for (int j = 0; j < affectedIndexes.size(); j++)
                    if (typesOfValuesToIndex.get(j) instanceof Integer)
                        affectedIndexes.get(j).put(row_id,
                                Integer.parseInt(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Double)
                        affectedIndexes.get(j).put(row_id,
                                Double.parseDouble(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Long)
                        affectedIndexes.get(j).put(row_id,
                                Long.parseLong(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Date)
                        try {
                            affectedIndexes.get(j).put(row_id,
                                    _format.parse(valuesToIndex.get(j)));
                        } catch (final ParseException e) {
                            throw new RuntimeException(
                                    "Parsing problem in StormThetaJoin.updatedIndexes "
                                            + e.getMessage());
                        }
                    else if (typesOfValuesToIndex.get(j) instanceof String)
                        affectedIndexes.get(j).put(row_id, valuesToIndex.get(j));
                    else
                        throw new RuntimeException("non supported type");

                valuesToIndexMap.put(key, valuesToIndex);

            } else if (secondRelationIndexes.containsKey(keyReverse)) {
                List<Index> affectedIndexes = secondRelationIndexes.get(keyReverse);

                final PredicateUpdateIndexesVisitor visitor = new PredicateUpdateIndexesVisitor(false, tuple);
                Predicate _joinPredicate = joinPredicates.get(keyReverse);
                _joinPredicate.accept(visitor);

                final List<Object> typesOfValuesToIndex = new ArrayList<Object>(
                        visitor._typesOfValuesToIndex);
                final List<String> valuesToIndex = new ArrayList<String>(
                        visitor._valuesToIndex);

                for (int j = 0; j < affectedIndexes.size(); j++)
                    if (typesOfValuesToIndex.get(j) instanceof Integer)
                        affectedIndexes.get(j).put(row_id,
                                Integer.parseInt(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Double)
                        affectedIndexes.get(j).put(row_id,
                                Double.parseDouble(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Long)
                        affectedIndexes.get(j).put(row_id,
                                Long.parseLong(valuesToIndex.get(j)));
                    else if (typesOfValuesToIndex.get(j) instanceof Date)
                        try {
                            affectedIndexes.get(j).put(row_id,
                                    _format.parse(valuesToIndex.get(j)));
                        } catch (final ParseException e) {
                            throw new RuntimeException(
                                    "Parsing problem in StormThetaJoin.updatedIndexes "
                                            + e.getMessage());
                        }
                    else if (typesOfValuesToIndex.get(j) instanceof String)
                        affectedIndexes.get(j).put(row_id, valuesToIndex.get(j));
                    else
                        throw new RuntimeException("non supported type");

                valuesToIndexMap.put(keyReverse, valuesToIndex);
            }
        }
    }

}

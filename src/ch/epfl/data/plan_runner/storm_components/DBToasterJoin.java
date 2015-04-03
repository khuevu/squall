package ch.epfl.data.plan_runner.storm_components;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.DBToasterApp;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Semaphore;


public class DBToasterJoin extends StormBoltComponent {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterJoin.class);

    //private final ProjectOperator _firstPreAggProj, _secondPreAggProj; // exists
    // only
    // for
    // preaggregations
    // performed on the output of the aggregationStorage

    private final ChainOperator _operatorChain;

    private long _numSentTuples = 0;

    // for load-balancing
    private final List<String> _fullHashList;

    // for batch sending
    private final Semaphore _semAgg = new Semaphore(1, true);
    private boolean _firstTime = true;
    private PeriodicAggBatchSend _periodicAggBatch;
    private final long _aggBatchOutputMillis;

    // for printing statistics for creating graphs
    protected Calendar _cal = Calendar.getInstance();
    protected DateFormat _statDateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    protected StatisticsUtilities _statsUtils;

    private DBToasterApp dbtoasterApp;
    private static final String DBT_GEN_PKG = "ddbt.gen.";
    private String _dbToasterQueryName;

    private StormEmitter[] _emitters;
    private Map<String, List<ColumnReference>> _indexedColRefs;

    public DBToasterJoin(StormEmitter[] emitters,
                         ComponentProperties cp, List<String> allCompNames,
                         //ProjectOperator firstPreAggProj, ProjectOperator secondPreAggProj,
                         Map<? extends StormEmitter, List<ColumnReference>> emitterColRefs,
                         int hierarchyPosition, TopologyBuilder builder,
                         TopologyKiller killer, Config conf) {
        super(cp, allCompNames, hierarchyPosition, conf);


        _emitters = emitters;
        _indexedColRefs = new HashMap<String, List<ColumnReference>>();
        for (StormEmitter e : _emitters) {
            String emitterIndex = String.valueOf(allCompNames.indexOf(e.getName()));
            List<ColumnReference> colRefs = emitterColRefs.get(e);
            _indexedColRefs.put(emitterIndex, colRefs);
        }

        _operatorChain = cp.getChainOperator();
        _fullHashList = cp.getFullHashList();

        _dbToasterQueryName = cp.getName() + "Impl";

        _aggBatchOutputMillis = cp.getBatchOutputMillis();

        _statsUtils = new StatisticsUtilities(getConf(), LOG);

        final int parallelism = SystemParameters.getInt(getConf(), getID()
                + "_PAR");

        // connecting with previous level
        InputDeclarer currentBolt = builder.setBolt(getID(), this, parallelism);
        if (MyUtilities.isManualBatchingMode(getConf()))
            currentBolt = MyUtilities.attachEmitterBatch(conf, _fullHashList,
                    currentBolt, _emitters[0], Arrays.copyOfRange(_emitters, 1, _emitters.length));
        else
            currentBolt = MyUtilities.attachEmitterHash(conf, _fullHashList,
                    currentBolt, _emitters[0], Arrays.copyOfRange(_emitters, 1, _emitters.length));

        // connecting with Killer
        if (getHierarchyPosition() == FINAL_COMPONENT
                && (!MyUtilities.isAckEveryTuple(conf)))
            killer.registerComponent(this, parallelism);
        if (cp.getPrintOut() && _operatorChain.isBlocking())
            currentBolt.allGrouping(killer.getID(),
                    SystemParameters.DUMP_RESULTS_STREAM);
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector collector) {
        super.prepare(map, tc, collector);

        dbtoasterApp = new DBToasterApp(DBT_GEN_PKG + _dbToasterQueryName);
    }


    @Override
    public void aggBatchSend() {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            if (_operatorChain != null) {
                final Operator lastOperator = _operatorChain.getLastOperator();
                if (lastOperator instanceof AggregateOperator) {
                    try {
                        _semAgg.acquire();
                    } catch (final InterruptedException ex) {
                    }

                    // sending
                    final AggregateOperator agg = (AggregateOperator) lastOperator;
                    final List<String> tuples = agg.getContent();
                    if (tuples != null) {
                        final String columnDelimiter = MyUtilities
                                .getColumnDelimiter(getConf());
                        for (String tuple : tuples) {
                            tuple = tuple.replaceAll(" = ", columnDelimiter);
                            tupleSend(
                                    MyUtilities.stringToTuple(tuple, getConf()),
                                    null, 0);
                        }
                    }

                    // clearing
                    agg.clearStorage();

                    _semAgg.release();
                }
            }
    }

    protected void applyOperatorsAndSend(Tuple stormTupleRcv,
                                         List<String> tuple, boolean isLastInBatch) {
        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            try {
                _semAgg.acquire();
            } catch (final InterruptedException ex) {
            }

        //System.out.println("BEFORE CHAIN Tuple: " + Arrays.toString(tuple.toArray()));
        tuple = _operatorChain.process(tuple);

        //System.out.println("AFTER CHAIN tuple: " + Arrays.toString(tuple.toArray()));

        if (MyUtilities.isAggBatchOutputMode(_aggBatchOutputMillis))
            _semAgg.release();

        if (tuple == null)
            return;
        _numSentTuples++;
        printTuple(tuple);

        if (_numSentTuples % _statsUtils.getDipOutputFreqPrint() == 0)
            printStatistics(SystemParameters.OUTPUT_PRINT);

        if (MyUtilities
                .isSending(getHierarchyPosition(), _aggBatchOutputMillis)) {
            long timestamp = 0;
            if (MyUtilities.isCustomTimestampMode(getConf()))
                timestamp = stormTupleRcv
                        .getLongByField(StormComponent.TIMESTAMP);
            tupleSend(tuple, stormTupleRcv, timestamp);
        }
        if (MyUtilities.isPrintLatency(getHierarchyPosition(), getConf())) {
            long timestamp;
            if (MyUtilities.isManualBatchingMode(getConf())) {
                if (isLastInBatch) {
                    timestamp = stormTupleRcv
                            .getLongByField(StormComponent.TIMESTAMP);
                    printTupleLatency(_numSentTuples - 1, timestamp);
                }
            } else {
                timestamp = stormTupleRcv
                        .getLongByField(StormComponent.TIMESTAMP);
                printTupleLatency(_numSentTuples - 1, timestamp);
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
            // the result is dumpped here
            System.out.println("Received Dumped Signal");
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

            if (processFinalAck(tuple, stormTupleRcv)) {
                System.out.println("Processing final ack");
                // need to close db toaster app here
                dbtoasterApp.endStream();
                return;
            }

            processNonLastTuple(inputComponentIndex, tuple, inputTupleHash,
                    stormTupleRcv, true);

        }
//        else {
//            final String inputComponentIndex = stormTupleRcv
//                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
//            final String inputBatch = stormTupleRcv
//                    .getStringByField(StormComponent.TUPLE);// getString(1);
//
//            final String[] wholeTuples = inputBatch
//                    .split(SystemParameters.MANUAL_BATCH_TUPLE_DELIMITER);
//            final int batchSize = wholeTuples.length;
//            for (int i = 0; i < batchSize; i++) {
//                // parsing
//                final String currentTuple = new String(wholeTuples[i]);
//                final String[] parts = currentTuple
//                        .split(SystemParameters.MANUAL_BATCH_HASH_DELIMITER);
//
//                String inputTupleHash = null;
//                String inputTupleString = null;
//                if (parts.length == 1)
//                    // lastAck
//                    inputTupleString = new String(parts[0]);
//                else {
//                    inputTupleHash = new String(parts[0]);
//                    inputTupleString = new String(parts[1]);
//                }
//                final List<String> tuple = MyUtilities.stringToTuple(
//                        inputTupleString, getConf());
//
//                // final Ack check
//                if (processFinalAck(tuple, stormTupleRcv)) {
//                    if (i != batchSize - 1)
//                        throw new RuntimeException(
//                                "Should not be here. LAST_ACK is not the last tuple!");
//                    return;
//                }
//
//                // processing a tuple
//                if (i == batchSize - 1)
//                    processNonLastTuple(inputComponentIndex, tuple,
//                            inputTupleHash, stormTupleRcv, true);
//                else
//                    processNonLastTuple(inputComponentIndex, tuple,
//                            inputTupleHash, stormTupleRcv, false);
//            }
//        }
        getCollector().ack(stormTupleRcv);
    }

    @Override
    public ChainOperator getChainOperator() {
        return _operatorChain;
    }

    // from IRichBolt
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return getConf();
    }

    // from StormComponent interface
    @Override
    public String getInfoID() {
        final String str = "DestinationStorage " + getID() + " has ID: "
                + getID();
        return str;
    }

    @Override
    protected InterchangingComponent getInterComp() {
        // should never be invoked
        return null;
    }

    // HELPER

    @Override
    public long getNumSentTuples() {
        return _numSentTuples;
    }

    @Override
    public PeriodicAggBatchSend getPeriodicAggBatch() {
        return _periodicAggBatch;
    }

    private static ColumnReference getColReference(int colIndex, List<ColumnReference> colRefs) {
        for (ColumnReference colRef : colRefs) {
            if (colRef.getColumnIndex() == colIndex) return colRef;
        }
        return null;
    }

    private List<List<String>> convertSnapshotToTuples(Object[] stream) {
        List<List<String>> tuples = new LinkedList<List<String>>();
        for (Object o : stream) {
            Object[] t = (Object[]) o;
            List<String> tuple = new LinkedList<String>();
            for (Object a : t) tuple.add("" + a);
            tuples.add(tuple);
        }
        return tuples;
    }

    protected void performJoin(Tuple stormTupleRcv, List<String> tuple,
                               String inputTupleHash,
                               List<ColumnReference> columnReferences,
                               boolean isLastInBatch) {

        List<Object> typedTuple = new ArrayList<Object>();

        //System.out.println("Inserting tuple from " + stormTupleRcv.getSourceComponent() + " : " + Arrays.toString(typedTuple.toArray()));
        for (int i = 0; i < tuple.size(); i++) {
            ColumnReference colRef = getColReference(i, columnReferences);
            Object t;
            if (colRef != null) {
                //System.out.println("For col: " + i + " have conversion of type: " + colRef.getType());
                t = colRef.eval(tuple);
            } else {
                t = tuple.get(i);
            }
            typedTuple.add(t);
        }

        System.out.println("Insert tuple: " + stormTupleRcv.getSourceComponent() + " " + Arrays.toString(typedTuple.toArray()));
        dbtoasterApp.insertTuple(stormTupleRcv.getSourceComponent(), typedTuple.toArray());

        Object[] stream = dbtoasterApp.getStream();
        List<List<String>> tuples = convertSnapshotToTuples(stream);

        for (List<String> outputTuple : tuples) {
//            System.out.println("output tuple: " + Arrays.toString(outputTuple.toArray()));
            applyOperatorsAndSend(stormTupleRcv, outputTuple, isLastInBatch);
        }

        //dbtoasterApp.getSnapShot();
        //final List<String> oppositeStringTupleList = oppositeStorage
        //        .access(inputTupleHash);

    }

    @Override
    protected void printStatistics(int type) {
        if (_statsUtils.isTestMode())
            if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT) {
                // computing variables
//                final int size1 = ((KeyValueStore<String, String>) _firstRelationStorage)
//                        .size();
//                final int size2 = ((KeyValueStore<String, String>) _secondRelationStorage)
//                        .size();
//                final int totalSize = size1 + size2;
                final String ts = _statDateFormat.format(_cal.getTime());

                // printing
                if (!MyUtilities.isCustomTimestampMode(getConf())) {
                    final Runtime runtime = Runtime.getRuntime();
                    final long memory = runtime.totalMemory()
                            - runtime.freeMemory();
                    if (type == SystemParameters.INITIAL_PRINT)
                        LOG.info(","
                                + "INITIAL,"
                                + _thisTaskID
                                + ","
                                + " TimeStamp:,"
                                + ts
//                                + ", FirstStorage:,"
//                                + size1
//                                + ", SecondStorage:,"
//                                + size2
//                                + ", Total:,"
//                                + totalSize
                                + ", Memory used: ,"
                                + StatisticsUtilities.bytesToMegabytes(memory)
                                + ","
                                + StatisticsUtilities.bytesToMegabytes(runtime
                                .totalMemory()));
                    else if (type == SystemParameters.INPUT_PRINT)
                        LOG.info(","
                                + "MEMORY,"
                                + _thisTaskID
                                + ","
                                + " TimeStamp:,"
                                + ts
//                                + ", FirstStorage:,"
//                                + size1
//                                + ", SecondStorage:,"
//                                + size2
//                                + ", Total:,"
//                                + totalSize
                                + ", Memory used: ,"
                                + StatisticsUtilities.bytesToMegabytes(memory)
                                + ","
                                + StatisticsUtilities.bytesToMegabytes(runtime
                                .totalMemory()));
                    else if (type == SystemParameters.OUTPUT_PRINT)
                        LOG.info("," + "RESULT," + _thisTaskID + ","
                                + "TimeStamp:," + ts + ",Sent Tuples,"
                                + getNumSentTuples());
                    else if (type == SystemParameters.FINAL_PRINT) {
                        if (numNegatives > 0)
                            LOG.info("WARNINGLAT! Negative latency for "
                                    + numNegatives + ", at most " + maxNegative
                                    + "ms.");
                        LOG.info(","
                                + "MEMORY,"
                                + _thisTaskID
                                + ","
                                + " TimeStamp:,"
                                + ts
//                                + ", FirstStorage:,"
//                                + size1
//                                + ", SecondStorage:,"
//                                + size2
//                                + ", Total:,"
//                                + totalSize
                                + ", Memory used: ,"
                                + StatisticsUtilities.bytesToMegabytes(memory)
                                + ","
                                + StatisticsUtilities.bytesToMegabytes(runtime
                                .totalMemory()));
                        LOG.info("," + "RESULT," + _thisTaskID + ","
                                + "TimeStamp:," + ts + ",Sent Tuples,"
                                + getNumSentTuples());
                    }
                } else // only final statistics is printed if we are measuring
                    // latency
                    if (type == SystemParameters.FINAL_PRINT) {
                        final Runtime runtime = Runtime.getRuntime();
                        final long memory = runtime.totalMemory()
                                - runtime.freeMemory();
                        if (numNegatives > 0)
                            LOG.info("WARNINGLAT! Negative latency for "
                                    + numNegatives + ", at most " + maxNegative
                                    + "ms.");
                        LOG.info(","
                                + "MEMORY,"
                                + _thisTaskID
                                + ","
                                + " TimeStamp:,"
                                + ts
                                + ", FirstStorage:,"
//                                + size1
//                                + ", SecondStorage:,"
//                                + size2
//                                + ", Total:,"
//                                + totalSize
                                + ", Memory used: ,"
                                + StatisticsUtilities.bytesToMegabytes(memory)
                                + ","
                                + StatisticsUtilities.bytesToMegabytes(runtime
                                .totalMemory()));
                        LOG.info("," + "RESULT," + _thisTaskID + ","
                                + "TimeStamp:," + ts + ",Sent Tuples,"
                                + getNumSentTuples());
                    }
            }
    }

    private void processNonLastTuple(String inputComponentIndex,
                                     List<String> tuple, String inputTupleHash, Tuple stormTupleRcv,
                                     boolean isLastInBatch) {

        boolean isFromFirstEmitter = false;
        BasicStore<ArrayList<String>> affectedStorage, oppositeStorage;
        ProjectOperator projPreAgg;
//        if (_firstEmitterIndex.equals(inputComponentIndex)) {
//            // R update
//            isFromFirstEmitter = true;
//            affectedStorage = _firstRelationStorage;
//            oppositeStorage = _secondRelationStorage;
//            projPreAgg = _secondPreAggProj;
//        } else if (_secondEmitterIndex.equals(inputComponentIndex)) {
//            // S update
//            isFromFirstEmitter = false;
//            affectedStorage = _secondRelationStorage;
//            oppositeStorage = _firstRelationStorage;
//            projPreAgg = _firstPreAggProj;
//        } else
//            throw new RuntimeException("InputComponentName "
//                    + inputComponentIndex + " doesn't match neither "
//                    + _firstEmitterIndex + " nor " + _secondEmitterIndex + ".");
//
//        // add the stormTuple to the specific storage
//        if (affectedStorage instanceof AggregationStorage)
//            // For preaggregations, we have to update the storage, not to insert
//            // to it
//            affectedStorage.update(tuple, inputTupleHash);
//        else {
//            final String inputTupleString = MyUtilities.tupleToString(tuple,
//                    getConf());
//            affectedStorage.insert(inputTupleHash, inputTupleString);
//        }

        List<ColumnReference> colRefs = _indexedColRefs.get(inputComponentIndex);
        performJoin(stormTupleRcv, tuple, inputTupleHash, colRefs, isLastInBatch);

//        if ((((KeyValueStore<String, String>) _firstRelationStorage).size() + ((KeyValueStore<String, String>) _secondRelationStorage)
//                .size()) % _statsUtils.getDipInputFreqPrint() == 0)
//            printStatistics(SystemParameters.INPUT_PRINT);
    }
}
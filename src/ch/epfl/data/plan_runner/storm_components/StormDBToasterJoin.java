package ch.epfl.data.plan_runner.storm_components;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import ch.epfl.data.plan_runner.components.ComponentProperties;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.DBToasterEngine;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.utilities.PeriodicAggBatchSend;
import ch.epfl.data.plan_runner.utilities.SystemParameters;
import ch.epfl.data.plan_runner.utilities.statistics.StatisticsUtilities;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Semaphore;


public class StormDBToasterJoin extends StormBoltComponent {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(StormDBToasterJoin.class);

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

    private DBToasterEngine dbtoasterEngine;
    private static final String DBT_GEN_PKG = "ddbt.gen.";
    private String _dbToasterQueryName;

    private StormEmitter[] _emitters;
    private Map<String, ValueExpression[]> _indexedColRefs;

    public StormDBToasterJoin(StormEmitter[] emitters,
                              ComponentProperties cp, List<String> allCompNames,
                              Map<String, ValueExpression[]> emitterNameColRefs,
                              int hierarchyPosition, TopologyBuilder builder,
                              TopologyKiller killer, Config conf) {
        super(cp, allCompNames, hierarchyPosition, conf);


        _emitters = emitters;
        _indexedColRefs = new HashMap<String, ValueExpression[]>();
        for (StormEmitter e : _emitters) {
            String emitterIndex = String.valueOf(allCompNames.indexOf(e.getName()));
            ValueExpression[] colRefs = emitterNameColRefs.get(e.getName());
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

        dbtoasterEngine = new DBToasterEngine(DBT_GEN_PKG + _dbToasterQueryName);
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

        tuple = _operatorChain.process(tuple);

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
            MyUtilities.dumpSignal(this, stormTupleRcv, getCollector());
            return;
        }

        if (!MyUtilities.isManualBatchingMode(getConf())) {
            final String inputComponentIndex = stormTupleRcv
                    .getStringByField(StormComponent.COMP_INDEX); // getString(0);
            final List<String> tuple = (List<String>) stormTupleRcv
                    .getValueByField(StormComponent.TUPLE); // getValue(1);

            if (processFinalAck(tuple, stormTupleRcv)) {
                // need to close db toaster app here
                dbtoasterEngine.endStream();
                return;
            }

            processNonLastTuple(inputComponentIndex, tuple,
                    stormTupleRcv, true);
        } else {
            throw new RuntimeException("Manual Batching Mode is not supported");
        }

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

    private List<List<String>> convertUpdateStreamToTuples(Object[] stream) {
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
                               ValueExpression[] columnReferences,
                               boolean isLastInBatch) {

        List<Object> typedTuple = new ArrayList<Object>();

        for (int i = 0; i < tuple.size(); i++) {
            ValueExpression ve = columnReferences[i];
            Object value = ve.eval(tuple);
            typedTuple.add(value);
        }

        //System.out.println("Insert tuple: " + stormTupleRcv.getSourceComponent() + " " + Arrays.toString(typedTuple.toArray()));
        dbtoasterEngine.insertTuple(stormTupleRcv.getSourceComponent(), typedTuple.toArray());

        Object[] stream = dbtoasterEngine.getStream();
        List<List<String>> tuples = convertUpdateStreamToTuples(stream);

        for (List<String> outputTuple : tuples) {
            applyOperatorsAndSend(stormTupleRcv, outputTuple, isLastInBatch);
        }

    }

    @Override
    protected void printStatistics(int type) {
        if (_statsUtils.isTestMode())
            if (getHierarchyPosition() == StormComponent.FINAL_COMPONENT) {
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
                                     List<String> tuple, Tuple stormTupleRcv,
                                     boolean isLastInBatch) {

        ValueExpression[] colRefs = _indexedColRefs.get(inputComponentIndex);
        performJoin(stormTupleRcv, tuple, colRefs, isLastInBatch);

    }
}
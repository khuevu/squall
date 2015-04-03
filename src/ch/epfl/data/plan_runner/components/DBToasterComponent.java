package ch.epfl.data.plan_runner.components;


import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.*;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.util.*;

public class DBToasterComponent implements Component {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterComponent.class);

//    private final Component _firstParent;
//    private final Component _secondParent;
    private Component _child;

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormJoin _joiner;

    private final ChainOperator _chain = new ChainOperator();

    // preAggregation
    private ProjectOperator _firstPreAggProj, _secondPreAggProj;

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut was already set

    private List<String> _fullHashList;
    private Predicate _joinPredicate;

    private List<Component> _parents;
    private Map<Component, List<ColumnReference>> _parentColRefs;

    private DBToasterComponent(List<Component> relations, Map<Component, List<ColumnReference>> relColRefs) {

        _parents = relations;
        _parentColRefs = relColRefs;
        StringBuilder nameBuilder = new StringBuilder();
        for (Component com : _parents) {
            com.setChild(this);

            if (nameBuilder.length() != 0) nameBuilder.append("_");
            nameBuilder.append(com.getName());
        }
        _componentName = nameBuilder.toString();
    }

    public static class Builder {
        private List<Component> relations = new LinkedList<Component>();
        private Map<Component, List<ColumnReference>> relColRefs = new HashMap<Component, List<ColumnReference>>();

        public Builder addRelation(Component relation, ColumnReference... columnReferences) {
            relations.add(relation);

            if (!relColRefs.containsKey(relation)) {
                relColRefs.put(relation, new LinkedList<ColumnReference>());
            }

            relColRefs.get(relation).addAll(Arrays.asList(columnReferences));
            return this;
        }

        public DBToasterComponent build() {
            return new DBToasterComponent(relations, relColRefs);
        }
    }

    @Override
    public DBToasterComponent add(Operator operator) {
        _chain.addOperator(operator);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Component)
            return _componentName.equals(((Component) obj).getName());
        else
            return false;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources() {
        final List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        for (final Component parent : getParents())
            list.addAll(parent.getAncestorDataSources());
        return list;
    }

    @Override
    public long getBatchOutputMillis() {
        return _batchOutputMillis;
    }

    @Override
    public ChainOperator getChainOperator() {
        return _chain;
    }

    @Override
    public Component getChild() {
        return _child;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
        return _joiner.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
        return _fullHashList;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return _hashExpressions;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return _hashIndexes;
    }

    @Override
    public String getInfoID() {
        return _joiner.getInfoID();
    }

    @Override
    public String getName() {
        return _componentName;
    }

    @Override
    public Component[] getParents() {
        return _parents.toArray(new Component[_parents.size()]);
    }

    @Override
    public boolean getPrintOut() {
        return _printOut;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash
                + (_componentName != null ? _componentName.hashCode() : 0);
        return hash;
    }

    public String getSQLQuery() {
        String sql = "CREATE STREAM CUSTOMER(CUSTKEY int, MKTSEGMENT String)\n" +
                "  FROM FILE '' LINE DELIMITED csv;\n" +
                "\n" +
                "CREATE STREAM ORDERS(ORDERKEY int, CUSTKEY int)\n" +
                "  FROM FILE '' LINE DELIMITED csv;\n" +
                "\n" +
                "SELECT CUSTOMER.MKTSEGMENT, COUNT(ORDERS.ORDERKEY)\n" +
                "FROM CUSTOMER join ORDERS on CUSTOMER.CUSTKEY=ORDERS.CUSTKEY\n" +
                "GROUP BY CUSTOMER.MKTSEGMENT";
        return sql;
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
                          List<String> allCompNames, Config conf, int partitioningType,
                          int hierarchyPosition) {

        // by default print out for the last component
        // for other conditions, can be set via setPrintOut
        if (hierarchyPosition == StormComponent.FINAL_COMPONENT
                && !_printOutSet)
            setPrintOut(true);

        MyUtilities.checkBatchOutput(_batchOutputMillis,
                _chain.getAggregation(), conf);

        // should issue a warning
        _joiner = new DBToasterJoin(getParents(), this,
                allCompNames,
                //_firstPreAggProj, _secondPreAggProj,
                _parentColRefs,
                hierarchyPosition,
                builder, killer, conf);
    }

    @Override
    public DBToasterComponent setBatchOutputMillis(long millis) {
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void setChild(Component child) {
        _child = child;
    }


    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public DBToasterComponent setFullHashList(List<String> fullHashList) {
        _fullHashList = fullHashList;
        return this;
    }

    @Override
    public DBToasterComponent setHashExpressions(
            List<ValueExpression> hashExpressions) {
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public DBToasterComponent setOutputPartKey(List<Integer> hashIndexes) {
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public DBToasterComponent setOutputPartKey(int... hashIndexes) {
        return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public DBToasterComponent setPrintOut(boolean printOut) {
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public Component setInterComp(InterchangingComponent inter) {
        throw new RuntimeException(
                "EquiJoin component does not support setInterComp");
    }

    @Override
    public DBToasterComponent setJoinPredicate(Predicate predicate) {
        _joinPredicate = predicate;
        return this;
    }

    @Override
    public Component setContentSensitiveThetaJoinWrapper(TypeConversion wrapper) {
        return this;
    }
}

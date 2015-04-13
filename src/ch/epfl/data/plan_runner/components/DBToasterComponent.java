package ch.epfl.data.plan_runner.components;


import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.conversion.*;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.*;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.sql.util.ParserUtil;
import ch.epfl.data.sql.visitors.jsql.SQLVisitor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBToasterComponent implements Component {

    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DBToasterComponent.class);


    private Component _child;

    private final String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormJoin _joiner;
    private Predicate _joinPredicate;

    private final ChainOperator _chain = new ChainOperator();

    private boolean _printOut;
    private boolean _printOutSet; // whether printOut was already set

    private List<String> _fullHashList;

    private List<Component> _parents;
    private Map<String, ValueExpression[]> _parentNameColRefs;

    private String _equivalentSQL;

    private DBToasterComponent(List<Component> relations, Map<String, ValueExpression[]> relColRefs, String sql) {

        _parents = relations;
        _parentNameColRefs = relColRefs;
        StringBuilder nameBuilder = new StringBuilder();
        for (Component com : _parents) {

            com.setChild(this);

            if (nameBuilder.length() != 0) nameBuilder.append("_");
            nameBuilder.append(com.getName());
        }
        _componentName = nameBuilder.toString();
        _equivalentSQL = sql;
    }

    public static class Builder {
        private List<Component> _relations = new LinkedList<Component>();
        private Map<String, ValueExpression[]> _relColRefs = new HashMap<String, ValueExpression[]>();
        private String _sql;
        private Pattern _sqlVarPattern = Pattern.compile("([A-Za-z0-9_]+)\\.f([0-9]+)");

        public Builder addRelation(Component relation, ColumnReference... columnReferences) {
            _relations.add(relation);
            ValueExpression[] cols = new ValueExpression[columnReferences.length];

            for (ColumnReference cref : columnReferences) {
                cols[cref.getColumnIndex()] = cref;
            }
            _relColRefs.put(relation.getName(), cols);
            return this;
        }

        private boolean parentRelationExists(String name) {
            boolean exist = false;
            for (Component rel : _relations) {
                if (rel.getName().equals(name)) {
                    exist = true;
                    break;
                }
            }
            return exist;
        }

        private void validateTables(List<Table> tables) {
            for (Table table : tables) {
                String tableName = table.getName();
                if (!parentRelationExists(tableName)) {
                    throw new RuntimeException("Invalid table name: " + tableName + " in the SQL query");
                }
            }
        }

        private void validateSelectItems(List<SelectItem> items) {
            for (SelectItem item : items) {
                String itemName = item.toString();
                Matcher matcher = _sqlVarPattern.matcher(itemName);
                while (matcher.find()) {
                    String tableName = matcher.group(1);
                    int fieldId = Integer.parseInt(matcher.group(2));

                    if (!parentRelationExists(tableName)) {
                        throw new RuntimeException("Invalid table name: " + tableName + " in the SQL query");
                    }

                    if (fieldId < 0 || fieldId >= _relColRefs.get(tableName).length) {
                        throw new RuntimeException("Invalid field f" + fieldId + " in table: " + tableName);
                    }

                }
            }

        }
        private void validateSQL(String sql) {
            SQLVisitor parsedQuery = ParserUtil.parseQuery("", sql);
            List<Table> tables = parsedQuery.getTableList();
            validateTables(tables);
            List<SelectItem> items = parsedQuery.getSelectItems();
            validateSelectItems(items);
        }

        private String getSQLTypeFromTypeConversion(TypeConversion typeConversion) {
            if (typeConversion instanceof LongConversion || typeConversion instanceof IntegerConversion) {
                return "int";
            } else if (typeConversion instanceof DoubleConversion) {
                return "float";
            } else if (typeConversion instanceof DateLongConversion) { // DBToaster code use Long for Date type.
                return "date";
            } else {
                return "String";
            }
        }

        private String generateSchemaSQL() {
            StringBuilder schemas = new StringBuilder();

            for (String relName : _relColRefs.keySet()) {
                schemas.append("CREATE STREAM ").append(relName).append("(");
                ValueExpression[] columnReferences = _relColRefs.get(relName);
                for (int i = 0; i < columnReferences.length; i++) {
                    schemas.append("f").append(i).append(" ").append(getSQLTypeFromTypeConversion(columnReferences[i].getType()));
                    if (i != columnReferences.length - 1) schemas.append(",");
                }
                schemas.append(") FROM FILE '' LINE DELIMITED csv;\n");
            }
            return schemas.toString();
        }

        public void setSQL(String sql) {
            validateSQL(sql);
            this._sql = generateSchemaSQL() + sql;
        }

        public DBToasterComponent build() {
            return new DBToasterComponent(_relations, _relColRefs, _sql);
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
        return _equivalentSQL;
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

        _joiner = new StormDBToasterJoin(getParents(), this,
                allCompNames,
                _parentNameColRefs,
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

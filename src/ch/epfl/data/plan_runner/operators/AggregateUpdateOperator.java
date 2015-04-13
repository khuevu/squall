package ch.epfl.data.plan_runner.operators;

import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.visitors.OperatorVisitor;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * This operator only stores input tuple into an aggregation storage and update the value when a new tuple which hashed to the same key is processed.
 *
 * It can be as the last agg for DBToasterComponent to maintain the updated state of the result given the input tuples is the stream of update
 * It is compatible with how Squall LocalMergeResult work.
 *
 * T must extends Number just because of the runAggregation(v1, v2) uses Addition. This function is called when LocalMergeResult
 * merge result from multiple nodes
 */
public class AggregateUpdateOperator<T extends Number & Comparable<T>> implements AggregateOperator<T> {

    private final AggregationStorage<T> _storage;
    private final TypeConversion<T> _wrapper;
    private final ValueExpression<T> _ve;
    private final Map _map;
    private List<Integer> _groupByColumns = new ArrayList<Integer>();
    private int _groupByType = GB_UNSET;
    // the GroupBy type
    private static final int GB_UNSET = -1;
    private static final int GB_COLUMNS = 0;

    private int _numTuplesProcessed = 0;



    public AggregateUpdateOperator(ValueExpression<T> ve, Map map) {
        _wrapper = ve.getType();
        _ve = ve;
        _map = map;
        _storage = new AggregationStorage<T>(this, _wrapper, _map, false);
    }
    @Override
    public void clearStorage() {
        _storage.reset();
    }

    @Override
    public DistinctOperator getDistinct() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ValueExpression> getExpressions() {
        final List<ValueExpression> result = new ArrayList<ValueExpression>();
        //result.add(_ve);
        return result;
    }

    @Override
    public List<Integer> getGroupByColumns() {
        return _groupByColumns;
    }

    @Override
    public ProjectOperator getGroupByProjection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BasicStore getStorage() {
        return _storage;
    }

    @Override
    public TypeConversion getType() {
        return _wrapper;
    }

    @Override
    public boolean hasGroupBy() {
        return true; // to work with LocalMergeResult
    }

    @Override
    public T runAggregateFunction(T value, List<String> tuple) {
        return _ve.eval(tuple);
    }

    @Override
    public T runAggregateFunction(T value1, T value2) {

        final ValueExpression<T> ve1 = new ValueSpecification<T>(_wrapper,
                value1);
        final ValueExpression<T> ve2 = new ValueSpecification<T>(_wrapper,
                value2);
        final Addition<T> result = new Addition<T>(ve1, ve2);
        return result.eval(null);
    }

    @Override
    public AggregateOperator setDistinct(DistinctOperator distinct) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setGroupByColumns(List<Integer> groupByColumns) {
        if (!alreadySetOther(GB_COLUMNS)) {
            _groupByType = GB_COLUMNS;
            _groupByColumns = groupByColumns;
            _storage.setSingleEntry(false);
            return this;
        } else
            throw new RuntimeException("Aggragation already has groupBy set!");
    }

    @Override
    public AggregateOperator setGroupByColumns(int... hashIndexes) {
        return setGroupByColumns(Arrays
                .asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public AggregateOperator setGroupByProjection(ProjectOperator projection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(OperatorVisitor ov) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getContent() {
        final String str = _storage.getContent();
        return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public int getNumTuplesProcessed() {
        return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    @Override
    public String printContent() {
        return _storage.getContent();
    }

    @Override
    public List<String> process(List<String> tuple) {
        _numTuplesProcessed++;
        String tupleHash = MyUtilities.createHashString(tuple, _groupByColumns,
                _map);
        //String tupleHash = tuple.get(0);
        _storage.update(tuple, tupleHash);
        // propagate further the affected tupleHash-tupleValue pair

        return tuple;
    }

    private boolean alreadySetOther(int GB_COLUMNS) {
        return (_groupByType != GB_COLUMNS && _groupByType != GB_UNSET);
    }
}

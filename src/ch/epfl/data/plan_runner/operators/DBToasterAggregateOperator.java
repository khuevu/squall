package ch.epfl.data.plan_runner.operators;

import ch.epfl.data.plan_runner.conversion.NumericConversion;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.Addition;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.storage.AggregationStorage;
import ch.epfl.data.plan_runner.storage.BasicStore;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import ch.epfl.data.plan_runner.visitors.OperatorVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by khuevu on 7/3/15.
 */
public class DBToasterAggregateOperator<T extends Number & Comparable<T>> implements AggregateOperator<T> {

    private final AggregationStorage<T> _storage;
    private final NumericConversion _wrapper;
    private final ValueExpression<T> _ve;
    private final Map _map;

    public DBToasterAggregateOperator(ValueExpression<T> ve, Map map) {
        _wrapper = (NumericConversion) ve.getType();
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
        result.add(_ve);
        return result;
    }

    @Override
    public List<Integer> getGroupByColumns() {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setDistinct(DistinctOperator distinct) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setGroupByColumns(List<Integer> groupByColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setGroupByColumns(int... groupByColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggregateOperator setGroupByProjection(ProjectOperator projection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void accept(OperatorVisitor ov) {

    }

    @Override
    public List<String> getContent() {
        final String str = _storage.getContent();
        return str == null ? null : Arrays.asList(str.split("\\r?\\n"));
    }

    @Override
    public int getNumTuplesProcessed() {
        return 0;
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

        String tupleHash = tuple.get(0);
        final T value = _storage.update(tuple, tupleHash);
        // propagate further the affected tupleHash-tupleValue pair

        return tuple;
    }
}

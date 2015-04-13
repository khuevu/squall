package ch.epfl.data.plan_runner.query_plans.dbtoaster;

import ch.epfl.data.plan_runner.components.DBToasterComponent;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.*;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateUpdateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;


/*
 * 	SELECT TOP 10 L_ORDERKEY, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE, O_ORDERDATE, O_SHIPPRIORITY
 FROM CUSTOMER, ORDERS, LINEITEM
 WHERE C_MKTSEGMENT = 'BUILDING' AND C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND
 O_ORDERDATE < '1995-03-15' AND L_SHIPDATE > '1995-03-15'
 GROUP BY L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
 ORDER BY REVENUE DESC, O_ORDERDATE
 */
public class DBToasterTPCH3Plan {
    private static Logger LOG = Logger.getLogger(DBToasterTPCH3Plan.class);

    private static final String _customerMktSegment = "BUILDING";
    private static final String _dateStr = "1995-03-15";

    private static final TypeConversion<Date> _dateConv = new DateConversion();
    private static final TypeConversion<Long> _dateLongConv = new DateLongConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final TypeConversion<Long> _lc = new LongConversion();
    private static final TypeConversion<Integer> _ic = new IntegerConversion();
    private static final Date _date = _dateConv.fromString(_dateStr);

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    public DBToasterTPCH3Plan(String dataPath, String extension, Map conf) {

        // -------------------------------------------------------------------------------------
        final List<Integer> hashCustomer = Arrays.asList(0);

        final SelectOperator selectionCustomer = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_sc, 6),
                        new ValueSpecification(_sc, _customerMktSegment)));

        final ProjectOperator projectionCustomer = new ProjectOperator(
                new int[]{0});

        final DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension)
                .setOutputPartKey(hashCustomer).add(selectionCustomer)
                .add(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashOrders = Arrays.asList(1);

        final SelectOperator selectionOrders = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.LESS_OP,
                        new ColumnReference(_dateConv, 4),
                        new ValueSpecification(_dateConv, _date)));

        final ProjectOperator projectionOrders = new ProjectOperator(new int[]{
                0, 1, 4, 7});

        final DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS", dataPath + "orders" + extension)
                .setOutputPartKey(hashOrders).add(selectionOrders)
                .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashLineitem = Arrays.asList(0);

        final SelectOperator selectionLineitem = new SelectOperator(
                new ComparisonPredicate(ComparisonPredicate.GREATER_OP,
                        new ColumnReference(_dateConv, 10),
                        new ValueSpecification(_dateConv, _date)));

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[]{0, 5, 6});

        final DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(hashLineitem).add(selectionLineitem)
                .add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        // -----------------------------------------------------------------------------------
        DBToasterComponent.Builder dbToasterCompBuilder = new DBToasterComponent.Builder();
        dbToasterCompBuilder.addRelation(relationCustomer, new ColumnReference(_lc, 0));
        dbToasterCompBuilder.addRelation(relationOrders, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1),
                new ColumnReference(_dateLongConv, 2), new ColumnReference(_lc, 3)); // Have to use DateLongConversion instead of DateConversion as DBToaster use Long as Date
        dbToasterCompBuilder.addRelation(relationLineitem, new ColumnReference(_lc, 0), new ColumnReference(_doubleConv, 1),
                new ColumnReference(_doubleConv, 2));
        dbToasterCompBuilder.setSQL("SELECT LINEITEM.f0, SUM(LINEITEM.f1 * (1 - LINEITEM.f2)) FROM CUSTOMER, ORDERS, LINEITEM " +
                "WHERE CUSTOMER.f0 = ORDERS.f1 AND ORDERS.f0 = LINEITEM.f0 " +
                "GROUP BY LINEITEM.f0, ORDERS.f2, ORDERS.f3");

        DBToasterComponent dbToasterComponent = dbToasterCompBuilder.build();

        AggregateOperator agg = new AggregateUpdateOperator(new ColumnReference(_doubleConv, 3), conf)
                .setGroupByColumns(Arrays.asList(0, 1, 2));
        dbToasterComponent.add(agg);

        _queryBuilder.add(dbToasterComponent);



        // Dummy component only print incoming tuples
//        DummyComponent dummyComponent = new DummyComponent(dbToasterComponent, "DUMMY");
//        _queryBuilder.add(dummyComponent);


    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }
}

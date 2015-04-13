package ch.epfl.data.plan_runner.query_plans.dbtoaster;

import ch.epfl.data.plan_runner.components.DBToasterComponent;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.*;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.expressions.DateSum;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.expressions.ValueSpecification;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.AggregateUpdateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;
import ch.epfl.data.plan_runner.operators.SelectOperator;
import ch.epfl.data.plan_runner.predicates.BetweenPredicate;
import ch.epfl.data.plan_runner.predicates.ComparisonPredicate;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *  SELECT N_NAME, SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS REVENUE
 FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
 WHERE C_CUSTKEY = O_CUSTKEY AND L_ORDERKEY = O_ORDERKEY AND L_SUPPKEY = S_SUPPKEY
 AND C_NATIONKEY = S_NATIONKEY AND S_NATIONKEY = N_NATIONKEY AND N_REGIONKEY = R_REGIONKEY
 AND R_NAME = 'ASIA' AND O_ORDERDATE >= '1994-01-01'
 AND O_ORDERDATE < DATEADD(YY, 1, cast('1994-01-01' as date))
 GROUP BY N_NAME
 ORDER BY REVENUE DESC
 */
public class DBToasterTPCH5Plan {

    private static Logger LOG = Logger.getLogger(DBToasterTPCH5Plan.class);

    private static final TypeConversion<Date> _dc = new DateConversion();
    private static final TypeConversion<Long> _lc = new LongConversion();
    private static final TypeConversion<String> _sc = new StringConversion();
    private static final NumericConversion<Double> _doubleConv = new DoubleConversion();

    private final QueryBuilder _queryBuilder = new QueryBuilder();

    // query variables
    private static Date _date1, _date2;
    private static final String REGION_NAME = "ASIA";

    private static void computeDates() {
        // date2 = date1 + 1 year
        final String date1Str = "1994-01-01";
        final int interval = 1;
        final int unit = Calendar.YEAR;

        // setting _date1
        _date1 = _dc.fromString(date1Str);

        // setting _date2
        ValueExpression<Date> date1Ve, date2Ve;
        date1Ve = new ValueSpecification<Date>(_dc, _date1);
        date2Ve = new DateSum(date1Ve, unit, interval);
        _date2 = date2Ve.eval(null);
        // tuple is set to null since we are computing based on constants
    }

    public DBToasterTPCH5Plan(String dataPath, String extension, Map conf) {
        computeDates();

        // -------------------------------------------------------------------------------------
        final List<Integer> hashRegion = Arrays.asList(0);

        final SelectOperator selectionRegion = new SelectOperator(
                new ComparisonPredicate(new ColumnReference(_sc, 1),
                        new ValueSpecification(_sc, REGION_NAME)));

        final ProjectOperator projectionRegion = new ProjectOperator(
                new int[] { 0 });

        final DataSourceComponent relationRegion = new DataSourceComponent(
                "REGION", dataPath + "region" + extension)
                .setOutputPartKey(hashRegion).add(selectionRegion)
                .add(projectionRegion);
        _queryBuilder.add(relationRegion);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashNation = Arrays.asList(2);

        final ProjectOperator projectionNation = new ProjectOperator(new int[] {
                0, 1, 2 });

        final DataSourceComponent relationNation = new DataSourceComponent(
                "NATION", dataPath + "nation" + extension).setOutputPartKey(
                hashNation).add(projectionNation);
        _queryBuilder.add(relationNation);

        // -------------------------------------------------------------------------------------

        final List<Integer> hashSupplier = Arrays.asList(1);

        final ProjectOperator projectionSupplier = new ProjectOperator(
                new int[] { 0, 3 });

        final DataSourceComponent relationSupplier = new DataSourceComponent(
                "SUPPLIER", dataPath + "supplier" + extension)
                .setOutputPartKey(hashSupplier).add(projectionSupplier);
        _queryBuilder.add(relationSupplier);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashLineitem = Arrays.asList(1);

        final ProjectOperator projectionLineitem = new ProjectOperator(
                new int[] { 0, 2, 5, 6 });

        final DataSourceComponent relationLineitem = new DataSourceComponent(
                "LINEITEM", dataPath + "lineitem" + extension)
                .setOutputPartKey(hashLineitem).add(projectionLineitem);
        _queryBuilder.add(relationLineitem);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashCustomer = Arrays.asList(0);

        final ProjectOperator projectionCustomer = new ProjectOperator(
                new int[] { 0, 3 });

        final DataSourceComponent relationCustomer = new DataSourceComponent(
                "CUSTOMER", dataPath + "customer" + extension)
                .setOutputPartKey(hashCustomer).add(projectionCustomer);
        _queryBuilder.add(relationCustomer);

        // -------------------------------------------------------------------------------------
        final List<Integer> hashOrders = Arrays.asList(1);

        final SelectOperator selectionOrders = new SelectOperator(
                new BetweenPredicate(new ColumnReference(_dc, 4), true,
                        new ValueSpecification(_dc, _date1), false,
                        new ValueSpecification(_dc, _date2)));

        final ProjectOperator projectionOrders = new ProjectOperator(new int[] {
                0, 1 });

        final DataSourceComponent relationOrders = new DataSourceComponent(
                "ORDERS", dataPath + "orders" + extension)
                .setOutputPartKey(hashOrders).add(selectionOrders)
                .add(projectionOrders);
        _queryBuilder.add(relationOrders);

        // -------------------------------------------------------------------------------------

        DBToasterComponent.Builder dbtBuilder = new DBToasterComponent.Builder();
        // Region: regionKey
        dbtBuilder.addRelation(relationRegion, new ColumnReference(_lc, 0));
        // Nation: nationKey, name, regionKey
        dbtBuilder.addRelation(relationNation, new ColumnReference(_lc, 0), new ColumnReference(_sc, 1), new ColumnReference(_lc, 2));
        // Supplier: supkey, nationKey
        dbtBuilder.addRelation(relationSupplier, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));
        // Lineitem: orderKey, supKey, extendedPrice, discount
        dbtBuilder.addRelation(relationLineitem, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1), new ColumnReference(_doubleConv, 2), new ColumnReference(_doubleConv, 3));
        // Customer: custKey, nationKey
        dbtBuilder.addRelation(relationCustomer, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));
        // Orders: orderKey, custKey
        dbtBuilder.addRelation(relationOrders, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));

        dbtBuilder.setSQL("SELECT NATION.f1, SUM(LINEITEM.f2 * (1 - LINEITEM.f3)) " +
                "FROM CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION " +
                "WHERE CUSTOMER.f0 = ORDERS.f1 AND LINEITEM.f0 = ORDERS.f0 AND LINEITEM.f1 = SUPPLIER.f0 " +
                "AND CUSTOMER.f1 = SUPPLIER.f1 AND SUPPLIER.f1 = NATION.f0 AND NATION.f2 = REGION.f0 " +
                "GROUP BY NATION.f1");

        DBToasterComponent dbtComp = dbtBuilder.build();

        AggregateOperator agg = new AggregateUpdateOperator(new ColumnReference(_doubleConv, 1), conf).setGroupByColumns(0);
        dbtComp.add(agg);
        _queryBuilder.add(dbtComp);

        // Dummy component only print incoming tuples
//        DummyComponent dummyComponent = new DummyComponent(dbtComp, "DUMMY");
//        _queryBuilder.add(dummyComponent);

        // -------------------------------------------------------------------------------------
    }

    public QueryBuilder getQueryPlan() {
        return _queryBuilder;
    }

}

package ch.epfl.data.plan_runner.query_plans;


import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DBToasterComponent;
import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.conversion.StringConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.operators.AggregateOperator;
import ch.epfl.data.plan_runner.operators.DBToasterAggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;

import java.util.Map;

public class HyracksDBToasterPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final LongConversion _lc = new LongConversion();
    private static final StringConversion _sc = new StringConversion();


    public HyracksDBToasterPlan(Map conf) {
        // -------------------------------------------------------------------------------------
        Component relationCustomer = _queryBuilder
                .createDataSource("customer", conf)
                .add(new ProjectOperator(0, 6)).setOutputPartKey(0);

        // -------------------------------------------------------------------------------------
        Component relationOrders = _queryBuilder
                .createDataSource("orders", conf).add(new ProjectOperator(0, 1))
                .setOutputPartKey(0);

        // -------------------------------------------------------------------------------------
        DBToasterComponent.Builder builder = new DBToasterComponent.Builder();
        builder.addRelation(relationCustomer, new ColumnReference(_lc, 0), new ColumnReference(_sc, 1));
        builder.addRelation(relationOrders, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));
        builder.setSQL("SELECT CUSTOMER.f1, COUNT(ORDERS.f0) FROM CUSTOMER, ORDERS WHERE CUSTOMER.f0 = ORDERS.f1 GROUP BY CUSTOMER.f1");

        DBToasterComponent dbToasterComponent = builder.build();

        //column 1 in agg constructor is refer to the output tuple while column 1 in setGroupBy refer to the input after join
        AggregateOperator agg = new DBToasterAggregateOperator<Long>(new ColumnReference<Long>(_lc, 1), conf);
        dbToasterComponent.add(agg);

        _queryBuilder.add(dbToasterComponent);
        // -------------------------------------------------------------------------------------
    }

    public QueryBuilder getQueryBuilder() {
        return _queryBuilder;
    }

}

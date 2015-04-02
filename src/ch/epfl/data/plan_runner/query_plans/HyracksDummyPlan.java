package ch.epfl.data.plan_runner.query_plans;


import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DBToasterComponent;
import ch.epfl.data.plan_runner.conversion.LongConversion;
import ch.epfl.data.plan_runner.expressions.ColumnReference;
import ch.epfl.data.plan_runner.operators.DBToasterAggregateOperator;
import ch.epfl.data.plan_runner.operators.ProjectOperator;

import java.util.Map;

public class HyracksDummyPlan {

    private final QueryBuilder _queryBuilder = new QueryBuilder();
    private static final LongConversion _lc = new LongConversion();


    public HyracksDummyPlan(Map conf) {
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
        builder.addRelation(relationCustomer, new ColumnReference(_lc, 0));
        builder.addRelation(relationOrders, new ColumnReference(_lc, 0), new ColumnReference(_lc, 1));
        DBToasterComponent dbToasterComponent = builder.build();

        dbToasterComponent.add(new DBToasterAggregateOperator<Long>(new ColumnReference<Long>(_lc, 1), conf));

        _queryBuilder.add(dbToasterComponent);
        //_queryBuilder.createEquiJoin(relationCustomer, relationOrders);
        // -------------------------------------------------------------------------------------
    }

    public QueryBuilder getQueryBuilder() {
        return _queryBuilder;
    }
}

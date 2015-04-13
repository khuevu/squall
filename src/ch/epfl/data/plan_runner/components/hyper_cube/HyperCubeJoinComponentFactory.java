package ch.epfl.data.plan_runner.components.hyper_cube;

import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.query_plans.QueryBuilder;
import java.util.ArrayList;

/**
 * Created by khayyam on 3/20/15.
 */
public class HyperCubeJoinComponentFactory {
    public static Component createHyperCubeJoinOperator(ArrayList<Component> parents, QueryBuilder queryBuilder) {
        Component result = null;
        result = new HyperCubeJoinStaticComponent(parents);
        queryBuilder.add(result);
        return result;
    }
}

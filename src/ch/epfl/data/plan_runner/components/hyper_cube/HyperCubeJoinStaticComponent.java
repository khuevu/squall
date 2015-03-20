package ch.epfl.data.plan_runner.components.hyper_cube;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import ch.epfl.data.plan_runner.components.Component;
import ch.epfl.data.plan_runner.components.DataSourceComponent;
import ch.epfl.data.plan_runner.conversion.TypeConversion;
import ch.epfl.data.plan_runner.expressions.ValueExpression;
import ch.epfl.data.plan_runner.operators.ChainOperator;
import ch.epfl.data.plan_runner.operators.Operator;
import ch.epfl.data.plan_runner.predicates.Predicate;
import ch.epfl.data.plan_runner.storm_components.InterchangingComponent;
import ch.epfl.data.plan_runner.storm_components.StormBoltComponent;
import ch.epfl.data.plan_runner.storm_components.StormComponent;
import ch.epfl.data.plan_runner.storm_components.synchronization.TopologyKiller;
import ch.epfl.data.plan_runner.storm_components.hyper_cube.StormHyperCubeJoin;
import ch.epfl.data.plan_runner.utilities.MyUtilities;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by khayyam on 3/20/15.
 */

public class HyperCubeJoinStaticComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(HyperCubeJoinStaticComponent.class);
    private ArrayList<Component> parents;
    private String componentName = "_";
    private long batchOutputMillis;
    private final ChainOperator chain = new ChainOperator();
    private Component child;
    private StormBoltComponent joiner;
    private List<ValueExpression> hashExpressions;
    private List<Integer> hashIndexes;
    private Predicate joinPredicate;
    private boolean printOut;
    private InterchangingComponent interComp = null;
    private boolean printOutSet; // whether printOut was already set
    private TypeConversion contentSensitiveThetaJoinWrapper = null;


    public HyperCubeJoinStaticComponent(ArrayList<Component> parents) {
        this.parents = parents;
        for (Component tmp : this.parents) {
            tmp.setChild(this);
            componentName += tmp.getName() + "_";
        }
    }

    @Override
    public HyperCubeJoinStaticComponent add(Operator operator) {
        chain.addOperator(operator);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Component)
            return componentName.equals(((Component) obj).getName());
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
        return batchOutputMillis;
    }

    @Override
    public ChainOperator getChainOperator() {
        return chain;
    }

    @Override
    public Component getChild() {
        return child;
    }

    // from StormEmitter interface
    @Override
    public String[] getEmitterIDs() {
        return joiner.getEmitterIDs();
    }

    @Override
    public List<String> getFullHashList() {
        throw new RuntimeException(
                "Load balancing for Theta join is done inherently!");
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return hashExpressions;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return hashIndexes;
    }

    @Override
    public String getInfoID() {
        return joiner.getInfoID();
    }

    public Predicate getJoinPredicate() {
        return joinPredicate;
    }

    @Override
    public String getName() {
        return componentName;
    }

    @Override
    public Component[] getParents() {
        return parents.toArray(new Component[parents.size()]);
    }

    @Override
    public boolean getPrintOut() {
        return printOut;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash
                + (componentName != null ? componentName.hashCode() : 0);
        return hash;
    }

    @Override
    public void makeBolts(TopologyBuilder builder, TopologyKiller killer,
                          List<String> allCompNames, Config conf, int partitioningType,
                          int hierarchyPosition) {
        MyUtilities.checkBatchOutput(batchOutputMillis,
		chain.getAggregation(), conf);

        /*************** Should be finished *****************/
        // _joiner = new StormThetaJoin();

    }
    @Override
    public HyperCubeJoinStaticComponent setBatchOutputMillis(long millis) {
        batchOutputMillis = millis;
        return this;
    }

    @Override
    public void setChild(Component child) {
        this.child = child;
    }

    // list of distinct keys, used for direct stream grouping and load-balancing
    // ()
    @Override
    public HyperCubeJoinStaticComponent setFullHashList(List<String> fullHashList) {
        throw new RuntimeException(
                "Load balancing for Theta join is done inherently!");
    }

    @Override
    public HyperCubeJoinStaticComponent setHashExpressions(
            List<ValueExpression> hashExpressions) {
        this.hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public HyperCubeJoinStaticComponent setOutputPartKey(List<Integer> hashIndexes) {
        this.hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public HyperCubeJoinStaticComponent setOutputPartKey(int... hashIndexes) {
        return setOutputPartKey(Arrays.asList(ArrayUtils.toObject(hashIndexes)));
    }

    @Override
    public HyperCubeJoinStaticComponent setInterComp(InterchangingComponent inter) {
        interComp = inter;
        return this;
    }

    @Override
    public HyperCubeJoinStaticComponent setJoinPredicate(Predicate joinPredicate) {
        this.joinPredicate = joinPredicate;
        return this;
    }

    @Override
    public HyperCubeJoinStaticComponent setPrintOut(boolean printOut) {
        this.printOutSet = true;
        this.printOut = printOut;
        return this;
    }

    @Override
    public HyperCubeJoinStaticComponent setContentSensitiveThetaJoinWrapper(
            TypeConversion wrapper) {
        contentSensitiveThetaJoinWrapper = wrapper;
        return this;
    }
}

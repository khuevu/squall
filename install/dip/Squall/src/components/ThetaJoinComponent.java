/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package components;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import expressions.ValueExpression;
import java.util.ArrayList;
import java.util.List;
import operators.AggregateOperator;
import operators.DistinctOperator;
import operators.ProjectionOperator;
import operators.SelectionOperator;
import stormComponents.StormThetaJoin;
import stormComponents.synchronization.TopologyKiller;
import org.apache.log4j.Logger;

import predicates.Predicate;
import queryPlans.QueryPlan;
import stormComponents.StormComponent;
import utilities.MyUtilities;

public class ThetaJoinComponent implements Component {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(ThetaJoinComponent.class);

    private Component _firstParent;
    private Component _secondParent;
    private Component _child;

    private String _componentName;

    private long _batchOutputMillis;

    private List<Integer> _hashIndexes;
    private List<ValueExpression> _hashExpressions;

    private StormThetaJoin _joiner;

    private SelectionOperator _selection;
    private DistinctOperator _distinct;
    private ProjectionOperator _projection;
    private AggregateOperator _aggregation;

    private boolean _printOut;
    private boolean _printOutSet; //whether printOut was already set
    
    private Predicate _joinPredicate;

    public ThetaJoinComponent(Component firstParent,
                    Component secondParent,
                    QueryPlan queryPlan){
      _firstParent = firstParent;
      _firstParent.setChild(this);
      _secondParent = secondParent;
      _secondParent.setChild(this);

      _componentName = firstParent.getName() + "_" + secondParent.getName();

      queryPlan.add(this);
    }
    
    public ThetaJoinComponent setJoinPredicate(Predicate joinPredicate) {
    	_joinPredicate = joinPredicate;
    	return this;
    }
    
    public Predicate getJoinPredicate() {
    	return _joinPredicate;
    }

    //list of distinct keys, used for direct stream grouping and load-balancing ()
    @Override
    public ThetaJoinComponent setFullHashList(List<String> fullHashList){
        throw new RuntimeException("Load balancing for Theta join is done inherently!");
    }

    @Override
    public List<String> getFullHashList(){
        throw new RuntimeException("Load balancing for Theta join is done inherently!");
    }

    @Override
    public ThetaJoinComponent setHashIndexes(List<Integer> hashIndexes){
        _hashIndexes = hashIndexes;
        return this;
    }

    @Override
    public ThetaJoinComponent setHashExpressions(List<ValueExpression> hashExpressions){
        _hashExpressions = hashExpressions;
        return this;
    }

    @Override
    public ThetaJoinComponent setSelection(SelectionOperator selection){
        _selection = selection;
        return this;
    }

    @Override
    public ThetaJoinComponent setDistinct(DistinctOperator distinct){
        _distinct = distinct;
        return this;
    }

    @Override
    public ThetaJoinComponent setProjection(ProjectionOperator projection){
        _projection = projection;
        return this;
    }

    @Override
    public ThetaJoinComponent setAggregation(AggregateOperator aggregation){
        _aggregation = aggregation;
        return this;
    }

    @Override
    public SelectionOperator getSelection() {
        return _selection;
    }

    @Override
    public DistinctOperator getDistinct() {
        return _distinct;
    }

    @Override
    public ProjectionOperator getProjection() {
        return _projection;
    }

    @Override
    public AggregateOperator getAggregation() {
        return _aggregation;
    }

    @Override
    public ThetaJoinComponent setPrintOut(boolean printOut){
        _printOutSet = true;
        _printOut = printOut;
        return this;
    }

    @Override
    public ThetaJoinComponent setBatchOutputMode(long millis){
        _batchOutputMillis = millis;
        return this;
    }

    @Override
    public void makeBolts(TopologyBuilder builder,
            TopologyKiller killer,
            Config conf,
            int partitioningType,
            int hierarchyPosition){

        //by default print out for the last component
        //for other conditions, can be set via setPrintOut
        if(hierarchyPosition==StormComponent.FINAL_COMPONENT && !_printOutSet){
           setPrintOut(true);
        }

        MyUtilities.checkBatchOutput(_batchOutputMillis, _aggregation, conf);

        _joiner = new StormThetaJoin(_firstParent,
                            _secondParent,
                            _componentName,
                            _selection,
                            _distinct,
                            _projection,
                            _aggregation,
                            _hashIndexes,
                            _hashExpressions,
                            _joinPredicate,
                            hierarchyPosition,
                            _printOut,
                            _batchOutputMillis,
                            builder,
                            killer,
                            conf);   
    }

    @Override
    public Component[] getParents() {
        return new Component[]{_firstParent, _secondParent};
    }

    @Override
    public Component getChild() {
        return _child;
    }

    @Override
    public void setChild(Component child) {
        _child = child;
    }

    @Override
    public int getOutputSize(){
        int joinColumnsLength = _firstParent.getHashIndexes().size();
        return _firstParent.getOutputSize() + _secondParent.getOutputSize() - joinColumnsLength;
    }

    @Override
    public List<DataSourceComponent> getAncestorDataSources(){
        List<DataSourceComponent> list = new ArrayList<DataSourceComponent>();
        for(Component parent: getParents()){
            list.addAll(parent.getAncestorDataSources());
        }
        return list;
    }

    // from StormEmitter interface
    @Override
    public int[] getEmitterIDs() {
         return _joiner.getEmitterIDs();
    }

    @Override
    public String getName() {
        return _componentName;
    }

    @Override
    public List<Integer> getHashIndexes() {
        return _hashIndexes;
    }

    @Override
    public List<ValueExpression> getHashExpressions() {
        return _hashExpressions;
    }

    @Override
    public String getInfoID() {
        return _joiner.getInfoID();
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof Component){
            return _componentName.equals(((Component)obj).getName());
        }else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + (this._componentName != null ? this._componentName.hashCode() : 0);
        return hash;
    }

}
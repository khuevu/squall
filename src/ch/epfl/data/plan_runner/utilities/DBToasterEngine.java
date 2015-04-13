package ch.epfl.data.plan_runner.utilities;

import org.apache.log4j.Logger;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import ddbt.lib.Messages.*;
import ddbt.lib.IQuery;


import java.io.Serializable;


public class DBToasterEngine implements Serializable {

    private static Logger LOG = Logger.getLogger(DBToasterEngine.class);

    public static final byte TUPLE_DELETE = 0x00;
    public static final byte TUPLE_INSERT = 0x01;

    private static final List EMPTY_LIST = List$.MODULE$.empty();

    public static List<Object> tuple(Object ... ts) {
        List<Object> result = EMPTY_LIST;
        for(int i = ts.length; i > 0; i--) {
            result = new $colon$colon<Object>(ts[i - 1], result);
        }
        return result;
    }

    private IQuery query; // DBToaster Query class

    public DBToasterEngine(String queryClass) {
        try {
            LOG.info("Loading Query class: " + queryClass);
            Class dbtoasterAppClass = this.getClass().getClassLoader().loadClass(queryClass);
            query = (IQuery) dbtoasterAppClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException("Fail to initialize Query class ", e);
        } catch (IllegalAccessException e) {
            LOG.error("", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class " + queryClass + " not found. ", e);
        }

    }

    public void insertTuple(String relationName, Object[] tuple) {
        List<Object> result = tuple(tuple);
        query.handleEvent(new TupleEvent(TUPLE_INSERT, relationName, result));
    }

    public Object[] getStream() {
        Object[] result = (Object[]) query.handleEvent(new GetStream(1));
        return result;
    }

    public void endStream() {
        query.handleEvent(EndOfStream$.MODULE$);
    }

}

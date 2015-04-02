package ch.epfl.data.plan_runner.utilities;

import ddbt.gen.*;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.List$;
import ddbt.lib.Messages.*;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions$;

import java.io.Serializable;

/**
 * Created by khuevu on 3/3/15.
 */
public class DBToasterApp implements Serializable {

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

    private QueryImpl query; // need a generic interface here

    public DBToasterApp(Class dbtoasterAppClass) {

        // Send events
        try {
            query = (QueryImpl) dbtoasterAppClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public void insertTuple(String relationName, Object[] tuple) {
        List<Object> result = tuple(tuple);
        query.handleEvent(new TupleEvent(TUPLE_INSERT, relationName, result));
    }

//    public java.util.Map<String, Object> getSnapShot() {
//        List<Map<String, Object>> result = query.getSnapShot();
//        Map<String, Object> map = result.apply(0);
//
//        return JavaConversions.asJavaMap(map);
//    }

    public Object[] getStream() {
        Object[] result = (Object[]) query.handleEvent(new GetStream(1));
        return result;
    }

    public void endStream() {

        query.handleEvent(EndOfStream$.MODULE$);

    }

}

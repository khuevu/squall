package ch.epfl.data.plan_runner.conversion;


public class DateLongConversion implements NumericConversion<Long> {
    private static final long serialVersionUID = 1L;

    private final DateConversion _dc = new DateConversion();

    @Override
    public Long fromDouble(double d) {
        return (long) d;
    }

    @Override
    public Long fromString(String str) {
        final String[] splits = str.split("-");
        final long year = Long.parseLong(new String(splits[0])) * 10000;
        final long month = Long.parseLong(new String(splits[1])) * 100;
        final long day = Long.parseLong(new String(splits[2]));
        return year + month + day;
    }

    @Override
    public double getDistance(Long bigger, Long smaller) {
        return _dc.getDistance(_dc.fromLong(bigger),
                _dc.fromLong(smaller));
    }

    @Override
    public Long getOffset(Object base, double delta) {
        return Long.valueOf(_dc.addDays((Integer) base, (int) delta));
    }

    @Override
    public Long getInitialValue() {
        return 0L;
    }

    @Override
    public Long minIncrement(Object obj) {
        return Long.valueOf(_dc.addDays((Integer) obj, 1));
    }

    @Override
    public Long minDecrement(Object obj) {
        return Long.valueOf(_dc.addDays((Integer) obj, -1));
    }

    @Override
    public Long getMinValue() {
        // return Integer.MIN_VALUE;
        return 18000101L;
    }

    @Override
    public Long getMinPositiveValue() {
        return 1L;
    }

    @Override
    public Long getMaxValue() {
        // return Integer.MAX_VALUE;
        return 20200101L;
    }

    @Override
    public double toDouble(Object obj) {
        return (Integer) obj;
    }

    @Override
    public String toString(Long obj) {
        return obj.toString();
    }

    public String toStringWithDashes(Integer obj) {
        String strDate = obj.toString();
        return strDate.substring(0, 4) + "-" + strDate.substring(4, 6) + "-"
                + strDate.substring(6, 8);
    }

    public static void main(String[] args) {
        DateIntegerConversion dcConv = new DateIntegerConversion();
        System.out.println(dcConv.getDistance(dcConv.getMaxValue(),
                dcConv.getMinValue()));
        System.out.println(dcConv.getDistance(dcConv.getMinValue(),
                dcConv.getMaxValue()));
        System.out.println(dcConv.getDistance(dcConv.getMaxValue(),
                dcConv.getMaxValue()));
        System.out.println(dcConv.getDistance(dcConv.getMinValue(),
                dcConv.getMinValue()));
        System.out.println(dcConv.minIncrement(dcConv.getMinValue()));
        System.out.println(dcConv.minDecrement(dcConv.getMaxValue()));
    }

}

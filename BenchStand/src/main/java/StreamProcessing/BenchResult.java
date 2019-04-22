package StreamProcessing;

public class BenchResult {
    final double _50;
    final double _75;
    final double _90;
    final double _99;
    final double timeTotal;
    final int throughPut;

    public BenchResult(double _50, double _75, double _90, double _99, double timeTotal,
                       int throughPut) {
        this._50 = _50;
        this._75 = _75;
        this._90 = _90;
        this._99 = _99;
        this.timeTotal = timeTotal;
        this.throughPut = throughPut;
    }

    @Override
    public String toString() {
        return _50 + "," + _75 + "," + _90 + "," + _99 + "," +
               timeTotal + "," + throughPut;
    }
}

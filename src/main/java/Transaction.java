import java.util.Map;

public class Transaction {
    private Map<String, Integer> keyValue;
    private Map<Integer, Integer> valueCount;

    public Transaction(Map<String, Integer> keyValueCache, Map<Integer, Integer> valueToCountCache) {
        this.keyValue = keyValueCache;
        this.valueCount = valueToCountCache;
    }

    public Map<String, Integer> getKeyValue() {
        return keyValue;
    }

    public Map<Integer, Integer> getValueCount() {
        return valueCount;
    }
}

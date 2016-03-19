import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Stack;

public class Database {

    private static Map<String, Integer> db = new HashMap<String, Integer>();
    private static Map<Integer, Integer> valueToCountDb = new HashMap<Integer, Integer>();
    private static Stack<Transaction> cache = new Stack<Transaction>();


    public static void main(String[] args) throws IOException {
        String path = "";
        if (args.length > 0){
            path = args[0];
            BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
            String line = "";
            while (!(line = bufferedReader.readLine()).equalsIgnoreCase("END")) {
                executeCommand(line);
            }
        }else {
            Scanner scanner = new Scanner(System.in);
            String line = "";
            while (!(line = scanner.nextLine()).equalsIgnoreCase("END")){
                executeCommand(line);
            }
        }
    }

    /**
     * This method executes the command
     * @param line
     */
    static void executeCommand(String line) {
        String[] command = line.split(" ");
        if (command[0].equalsIgnoreCase("BEGIN"))
            begin();
        else if (command[0].equalsIgnoreCase("SET"))
            if (cache.empty())
                setDb(command[1], (Integer.parseInt(command[2])));
            else setCache(command[1], (Integer.parseInt(command[2])));
        else if (command[0].equalsIgnoreCase("UNSET"))
            if (cache.empty())
                unsetDb(command[1]);
            else unsetCache(command[1]);
        else if (command[0].equalsIgnoreCase("NUMEQUALTO"))
            if (cache.empty())
                numEqualDb(Integer.parseInt(command[1]));
            else numEqualCache(Integer.parseInt(command[1]));
        else if (command[0].equalsIgnoreCase("GET"))
            if (cache.empty())
                getFromDb(command[1]);
            else getFromCache(command[1]);
        else if (command[0].equalsIgnoreCase("ROLLBACK"))
            rollback();
        else
            if (command[0].equalsIgnoreCase("COMMIT"))
               commit();
    }

    /**
     * Set the values to the database
     * @param key
     * @param value
     */
    public static void setDb(String key, Integer value) {
        if (db.containsKey(key)) {
            if (db.get(key) != value) {
                valueToCountDb.put(db.get(key), valueToCountDb.get(db.get(key)) - 1);
            } else {
                return;
            }
        }
        if (valueToCountDb.containsKey(value)) {
            valueToCountDb.put(value, valueToCountDb.get(value) + 1);
        } else {
            valueToCountDb.put(value, 1);
        }
        db.put(key, value);
    }

    /**
     * Unset the variables from the database
     * @param key
     */
    public static void unsetDb(String key) {
        if (db.containsKey(key)) {
            valueToCountDb.put(db.get(key), valueToCountDb.get(db.get(key)) - 1);
            db.remove(key);
        }
    }

    /**
     * Get the value of the variable/key from the database
     * @param key
     */
    public static void getFromDb(String key) {
        if (db.containsKey(key))
            System.out.println(db.get(key));
        else System.out.println("NULL");
    }

    /**
     * Prints the number of keys/variables that have the given value
     * @param value
     */
    public static void numEqualDb(int value) {
        if (valueToCountDb.containsKey(value))
            System.out.println(valueToCountDb.get(value));
        else System.out.println(0);
    }

    /**
     * Handles the start of a database transaction
     */
    private static void begin() {
        if (cache.empty()) {
            cache.push(new Transaction(new HashMap<String, Integer>(), new HashMap<Integer, Integer>(valueToCountDb)));
        } else {
            Transaction transaction = cache.peek();
            Map<String, Integer> keyValue = new HashMap<String, Integer>(transaction.getKeyValue());
            Map<Integer, Integer> valueCount = new HashMap<Integer, Integer>(transaction.getValueCount());
            cache.push(new Transaction(keyValue, valueCount));
        }
    }


    /**
     * Set the key/value of the transaction in a temporary cache
     * @param key
     * @param val
     */
    public static void setCache(String key, Integer val) {
        Transaction transaction = cache.pop();
        if (transaction.getKeyValue().containsKey(key))  // If key already in cache
            updateCount(transaction.getValueCount(), transaction.getKeyValue().get(key), val);
        else if (db.get(key) != null)                         // If key is in db
            updateCount(transaction.getValueCount(), db.get(key), val);
        else {                                                 // If key is neither in cache nor in db
            if (transaction.getValueCount().containsKey(val))
                transaction.getValueCount().put(val, transaction.getValueCount().get(val) + 1);
            else transaction.getValueCount().put(val, 1);
        }
        transaction.getKeyValue().put(key, val);
        cache.push(transaction);
    }

    /**
     * Helper function to update the count of keys having a specific value in a transaction. This is called each time when
     * SET is called within the transaction
     * @param map
     * @param oldVal
     * @param newVal
     */
    private static void updateCount(Map<Integer, Integer> map, int oldVal, int newVal) {
        if (oldVal != newVal) {
            if (map.containsKey(newVal))
                map.put(newVal, map.get(newVal) + 1);
            else map.put(newVal, 1);
            map.put(oldVal, map.get(oldVal) - 1);
        }
    }

    /**
     * Unset the key/variable in a transaction
     * @param key
     */
    public static void unsetCache(String key) {
        Transaction transaction = cache.pop();
        if (transaction.getKeyValue().containsKey(key)) {
            int value = transaction.getKeyValue().get(key);
            transaction.getValueCount().put(value, transaction.getValueCount().get(value) - 1);
        } else if (db.get(key) != null) {
            int count = transaction.getValueCount().get(db.get(key));
            transaction.getValueCount().put(db.get(key), count - 1);
        } else return;
        transaction.getKeyValue().put(key, null);
        cache.push(transaction);
    }

    /**
     * Handles NUMEQUALTO command within a transaction
     * @param value
     */
    public static void numEqualCache(int value) {
        System.out.println(cache.peek().getValueCount().get(value));
    }

    public static void getFromCache(String key) {
        if (cache.peek().getKeyValue().containsKey(key))
            System.out.println(cache.peek().getKeyValue().get(key));
        else getFromDb(key);
    }

    /**
     * Handles ROLLBACK within a transaction
     */
    public static void rollback() {
        if (cache.empty()) {
            System.out.println("NO TRANSACTION");
        } else {
            cache.pop();
        }
    }

    /**
     * Commit all the changes to the database
     */
    public static void commit() {
        if (!cache.empty()) {
            Transaction transaction = cache.pop();
            Map<String, Integer> keyValues = transaction.getKeyValue();
            Map<Integer, Integer> valueCount = transaction.getValueCount();
            for (Map.Entry<String, Integer> entry : keyValues.entrySet()) {
                if (entry.getValue() == null)
                    db.remove(entry.getKey());
                else db.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Integer, Integer> entry : valueCount.entrySet()) {
                valueToCountDb.put(entry.getKey(), entry.getValue());
            }
            while (!cache.empty()) {
                cache.pop();
            }
        }
    }
}

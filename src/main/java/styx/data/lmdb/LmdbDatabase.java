package styx.data.lmdb;

import static org.fusesource.lmdbjni.Constants.NOSUBDIR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

import styx.data.db.Path;
import styx.data.db.Row;

class LmdbDatabase implements styx.data.db.Database {

    private static final Map<String, LmdbDatabase> namedInstances = new HashMap<>();

    private final Env env = new Env();

    private LmdbDatabase(String path) {
        try {
            Files.createDirectories(Paths.get(path).getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.env.open(path, NOSUBDIR);
    }

    static LmdbDatabase open(String name) {
        synchronized (namedInstances) {
            if(name == null || name.isEmpty()) {
                throw new IllegalArgumentException();
            } else {
                LmdbDatabase instance = namedInstances.get(name);
                if(instance == null) {
                    instance = new LmdbDatabase(name);
                    namedInstances.put(name, instance);
                }
                return instance;
            }
        }
    }

    @Override
    public void close() {
        // this.env.close();
    }

    @Override
    public Stream<Row> selectAll() {
        try (Transaction tx = env.createReadTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                List<Row> rows = new ArrayList<>();
                if(cursor.first()) {
                    do {
                        rows.add(readRow(cursor));
                    } while(cursor.next());
                }
                return rows.stream();
            }
        }
    }

    @Override
    public Optional<Row> selectSingle(Path parent, String key) {
        try (Transaction tx = env.createReadTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                writeKey(cursor, parent, key);
                if(cursor.seekKey()) {
                    return Optional.of(readRow(cursor));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    @Override
    public Stream<Row> selectChildren(Path parent) {
        try (Transaction tx = env.createReadTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                byte[] keyPrefix = writeKeyPrefix(cursor, parent, true);
                List<Row> rows = new ArrayList<>();
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, keyPrefix)) {
                            break;
                        }
                        rows.add(readRow(cursor));
                    } while(cursor.next());
                }
                return rows.stream();
            }
        }
    }

    @Override
    public Stream<Row> selectDescendants(Path parent) {
        try (Transaction tx = env.createReadTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                byte[] keyPrefix = writeKeyPrefix(cursor, parent, false);
                List<Row> rows = new ArrayList<>();
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, keyPrefix)) {
                            break;
                        }
                        rows.add(readRow(cursor));
                    } while(cursor.next());
                }
                return rows.stream().sorted(Row.ITERATION_ORDER);
            }
        }
    }

    @Override
    public int allocateSuffix(Path parent) {
        try (Transaction tx = env.createReadTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                byte[] keyPrefix = writeKeyPrefix(cursor, parent, true);
                int maxSuffix = 0;
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, keyPrefix)) {
                            break;
                        }
                        maxSuffix = Math.max(maxSuffix, readSuffix(cursor));
                    } while(cursor.next());
                }
                return maxSuffix + 1;
            }
        }
    }

    @Override
    public void insert(Row row) {
        try (Transaction tx = env.createWriteTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                writeKey(cursor, row.parent(), row.key());
                if(cursor.seekKey()) {
                    throw new IllegalStateException();
                }
                writeValue(cursor, row.suffix(), row.value());
                cursor.put();
            }
            tx.commit();
        }
    }

    @Override
    public void deleteAll() {
        try (Transaction tx = env.createWriteTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                if(cursor.first()) {
                    do {
                        cursor.delete();
                    } while(cursor.next());
                }
            }
            tx.commit();
        }
    }

    @Override
    public void deleteSingle(Path parent, String key) {
        try (Transaction tx = env.createWriteTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                writeKey(cursor, parent, key);
                if(cursor.seekKey()) {
                    cursor.delete();
                }
            }
            tx.commit();
        }
    }

    @Override
    public void deleteDescendants(Path parent) {
        try (Transaction tx = env.createWriteTransaction(); Database db = env.openDatabase(tx, null, 0)) {
            try(BufferCursor cursor = db.bufferCursor(tx)) {
                byte[] keyPrefix = writeKeyPrefix(cursor, parent, false);
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, keyPrefix)) {
                            break;
                        }
                        cursor.delete();
                    } while(cursor.next());
                }
            }
            tx.commit();
        }
    }

    private static boolean startsWith(BufferCursor cursor, byte[] keyPrefix) {
        int keyLength = cursor.keyLength();
        if(keyLength < keyPrefix.length) {
            return false;
        }
        for(int i = 0; i < keyPrefix.length; i++) {
            if(cursor.keyByte(i) != keyPrefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static Row readRow(BufferCursor cursor) {
        int keyLength = cursor.keyLength();
        int keySepPos = 0;
        while(keySepPos < keyLength && cursor.keyByte(keySepPos) != '\t') {
            keySepPos++;
        }
        if(keySepPos == keyLength) {
            throw new IllegalStateException();
        }
        Path parent = Path.decode(new String(cursor.keyBytes(0, keySepPos), StandardCharsets.UTF_8));
        String key = new String(cursor.keyBytes(keySepPos+1, keyLength-keySepPos-1), StandardCharsets.UTF_8);
        if(cursor.valByte(0) == 'S') {
            String value = new String(cursor.valBytes(1, cursor.valLength()-1), StandardCharsets.UTF_8);
            return new Row(parent, key, 0, value);
        } else if(cursor.valByte(0) == 'C') {
            int suffix = cursor.valInt(1);
            return new Row(parent, key, suffix, null);
        } else {
            throw new IllegalStateException();
        }
    }

    private static int readSuffix(BufferCursor cursor) {
        if(cursor.valByte(0) == 'C') {
            return cursor.valInt(1);
        } else {
            return 0;
        }
    }

    private static void writeKey(BufferCursor cursor, Path parent, String key) {
        cursor.keyWriteBytes(parent.encode().getBytes(StandardCharsets.UTF_8));
        cursor.keyWriteByte('\t');
        cursor.keyWriteBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] writeKeyPrefix(BufferCursor cursor, Path parent, boolean sep) {
        String string = parent.encode();
        if(sep) {
            string += "\t";
        }
        byte[] keyPrefix = string.getBytes(StandardCharsets.UTF_8);
        cursor.keyWriteBytes(keyPrefix);
        return keyPrefix;
    }

    private static void writeValue(BufferCursor cursor, int suffix, String value) {
        if(value != null) {
            cursor.valWriteByte('S');
            cursor.valWriteBytes(value.getBytes(StandardCharsets.UTF_8));
        } else {
            cursor.valWriteByte('C');
            cursor.valWriteInt(suffix);
        }
    }
}

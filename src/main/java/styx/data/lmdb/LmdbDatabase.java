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
                        rows.add(createRow(cursor));
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
                cursor.keyWriteBytes((parent.encode() + "\t" + key).getBytes(StandardCharsets.UTF_8));
                if(cursor.seekKey()) {
                    return Optional.of(createRow(cursor));
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
                byte[] prefix = (parent.encode() + "\t").getBytes(StandardCharsets.UTF_8);
                cursor.keyWriteBytes(prefix);
                List<Row> rows = new ArrayList<>();
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, prefix)) {
                            break;
                        }
                        rows.add(createRow(cursor));
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
                byte[] prefix = parent.encode().getBytes(StandardCharsets.UTF_8);
                cursor.keyWriteBytes(prefix);
                List<Row> rows = new ArrayList<>();
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, prefix)) {
                            break;
                        }
                        rows.add(createRow(cursor));
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
                byte[] prefix = (parent.encode() + "\t").getBytes(StandardCharsets.UTF_8);
                cursor.keyWriteBytes(prefix);
                int maxSuffix = 0;
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, prefix)) {
                            break;
                        }
                        maxSuffix = Math.max(maxSuffix, createRow(cursor).suffix()); // TODO (optimize) extract suffix
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
                cursor.keyWriteBytes((row.parent().encode() + "\t" + row.key()).getBytes(StandardCharsets.UTF_8));
                if(cursor.seekKey()) {
                    throw new IllegalStateException();
                }
                cursor.valWriteBytes((row.suffix() + "\t" + (row.value() == null ? "" : row.value())).getBytes(StandardCharsets.UTF_8));
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
                cursor.keyWriteBytes((parent.encode() + "\t" + key).getBytes(StandardCharsets.UTF_8));
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
                byte[] prefix = parent.encode().getBytes(StandardCharsets.UTF_8);
                cursor.keyWriteBytes(prefix);
                if(cursor.seekRange()) {
                    do {
                        if(!startsWith(cursor, prefix)) {
                            break;
                        }
                        cursor.delete();
                    } while(cursor.next());
                }
            }
            tx.commit();
        }
    }

    private static boolean startsWith(BufferCursor cursor, byte[] prefix) {
        byte[] key = cursor.keyBytes();
        if(key.length < prefix.length) {
            return false;
        }
        for(int i = 0; i < prefix.length; i++) {
            if(key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private static Row createRow(BufferCursor cursor) {
//      byte[] key = cursor.keyBytes();
//      int suffix = cursor.valInt(0);
//      byte[] value = cursor.valBytes(4, cursor.valLength() - 4);
//      int pos = 0;
//      while(key[pos] != 0) pos++;
//      rows.add(new Row(
//              Path.decode(new String(key, 0, pos, StandardCharsets.UTF_8)),
//              new String(key, pos+1, key.length-pos-1, StandardCharsets.UTF_8),
//              suffix,
//              new String(value, StandardCharsets.UTF_8)));
      String key = new String(cursor.keyBytes(), StandardCharsets.UTF_8);
      String val = new String(cursor.valBytes(), StandardCharsets.UTF_8);
      int p1 = key.indexOf('\t');
      int p2 = val.indexOf('\t');

      return new Row(
              Path.decode(key.substring(0, p1)),
              key.substring(p1+1),
              Integer.parseInt(val.substring(0, p2)),
              p2+1 == val.length() ? null : val.substring(p2+1));
    }
}

package styx.data.lmdb;

import static org.fusesource.lmdbjni.Constants.NOSUBDIR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import styx.data.db.DatabaseTransaction;

class LmdbDatabase implements styx.data.db.Database {

    private static final Map<String, LmdbDatabase> namedInstances = new HashMap<>();

    // TODO: ENV is never closed (files MUST only be opened once per process)!
    //
    // See http://www.lmdb.tech/doc/index.html:
    // Do not have open an LMDB database twice in the same process at the same time.
    // Not even from a plain open() call - close()ing it breaks flock() advisory locking.

    private final Env env = new Env();
    private final Database dbi;

    private LmdbDatabase(String path) {
        try {
            Files.createDirectories(Paths.get(path).getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.env.open(path, NOSUBDIR);
        this.dbi = env.openDatabase(null, 0);
    }

    static LmdbDatabase open(String name) {
        Objects.requireNonNull(name);
        synchronized (namedInstances) {
            LmdbDatabase instance = namedInstances.get(name);
            if(instance == null) {
                instance = new LmdbDatabase(name);
                namedInstances.put(name, instance);
            }
            return instance;
        }
    }

    @Override
    public void close() { }

    @Override
    public DatabaseTransaction openReadTransaction() {
        return new LmdbTransaction(env.createReadTransaction(), dbi);
    }

    @Override
    public DatabaseTransaction openWriteTransaction() {
        return new LmdbTransaction(env.createWriteTransaction(), dbi);
    }
}

package styx.data.lmdb;

import java.util.Optional;

import styx.data.Store;
import styx.data.StoreProvider;
import styx.data.db.DatabaseStore;

public class LmdbStoreProvider implements StoreProvider {

    @Override
    public Optional<Store> openStore(String url) {
        if(url.startsWith("lmdb:")) {
            return Optional.of(new DatabaseStore(LmdbDatabase.open(url.substring(5))));
        } else {
            return Optional.empty();
        }
    }
}

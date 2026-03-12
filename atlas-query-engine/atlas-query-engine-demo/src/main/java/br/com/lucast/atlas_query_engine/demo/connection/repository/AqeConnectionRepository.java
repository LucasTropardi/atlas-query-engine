package br.com.lucast.atlas_query_engine.demo.connection.repository;

import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import java.util.Optional;

public interface AqeConnectionRepository {

    AqeConnectionEntity save(AqeConnectionEntity entity);

    Optional<AqeConnectionEntity> findByConnectionKey(String connectionKey);
}

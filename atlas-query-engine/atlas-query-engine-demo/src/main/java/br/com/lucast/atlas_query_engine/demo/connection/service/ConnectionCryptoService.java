package br.com.lucast.atlas_query_engine.demo.connection.service;

public interface ConnectionCryptoService {

    String encrypt(String value);

    String decrypt(String value);
}

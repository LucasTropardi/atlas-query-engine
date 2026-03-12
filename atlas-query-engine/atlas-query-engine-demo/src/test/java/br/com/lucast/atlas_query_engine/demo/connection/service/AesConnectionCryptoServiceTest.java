package br.com.lucast.atlas_query_engine.demo.connection.service;

import br.com.lucast.atlas_query_engine.demo.config.AtlasSecurityProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AesConnectionCryptoServiceTest {

    @Test
    void shouldEncryptAndDecryptConnectionFields() {
        AtlasSecurityProperties properties = new AtlasSecurityProperties();
        properties.setEncryptionKey("test-encryption-key");

        AesConnectionCryptoService cryptoService = new AesConnectionCryptoService(properties);
        cryptoService.initialize();

        String encrypted = cryptoService.encrypt("sensitive-value");

        assertThat(encrypted).isNotBlank();
        assertThat(encrypted).isNotEqualTo("sensitive-value");
        assertThat(cryptoService.decrypt(encrypted)).isEqualTo("sensitive-value");
    }
}

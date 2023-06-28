package uk.gov.digital.ho.hocs.hocstxadocumentextractor.batch;

import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfiguration {
    /*
    Configuration for the data sources of the Spring Batch job.
     */

    @Bean
    @Primary
    @ConfigurationProperties("spring-batch-db.datasource")
    DataSource springBatchDb() {
        /*
        An in-memory database for Spring Batch to read/write job
        execution metadata to. Different to the data sources used for
        actual document metadata.
         */
        DataSourceBuilder builder = DataSourceBuilder.create();
        builder.type(HikariDataSource.class);
        return builder.build();
    }

    @Bean("metadataSource")
    @ConfigurationProperties("document-metadata")
    public DataSource metadataSource() {
        /*
        The data source containing the metadata of documents we wish
        to extract.
        DataSource settings are set directly from the document-metadata section of
        the application.yml
         */
        PGSimpleDataSource metaSource = new PGSimpleDataSource();
        return metaSource;
    }

}

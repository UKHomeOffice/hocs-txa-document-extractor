package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class HocsTxaDocumentExtractorApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(HocsTxaDocumentExtractorApplication.class, args);
    }
}

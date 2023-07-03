package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("integration") // stops the job being launched during unit test
public class HocsTxaDocumentExtractorApplicationTest {

    @Test
    public void contextLoads() {
        assertTrue(true);
    }
}

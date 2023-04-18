package uk.gov.digital.ho.hocs.hocstxadocumentextractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.stream.Stream;

@SpringBootApplication
public class HocsTxaDocumentExtractorApplication {

    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(HocsTxaDocumentExtractorApplication.class, args);
        displayAllBeans(applicationContext);
    }

    public static void displayAllBeans(ApplicationContext applicationContext) {
        System.out.println("PRINT ALL BEANS ------");
        String[] allBeanNames = applicationContext.getBeanDefinitionNames();
        allBeanNames = Stream.of(allBeanNames)
            .sorted()
            .toArray(String[]::new);
        for(String beanName : allBeanNames) {
            System.out.println(beanName);
        }
    }

}

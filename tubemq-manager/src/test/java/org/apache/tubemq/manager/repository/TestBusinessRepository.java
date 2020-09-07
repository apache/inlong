package org.apache.tubemq.manager.repository;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.tubemq.manager.entry.BusinessEntry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@DataJpaTest
public class TestBusinessRepository {
    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private BusinessRepository businessRepository;

    @Test
    public void whenFindByNameThenReturnBusiness() {
        BusinessEntry businessEntry = new BusinessEntry();
        businessEntry.setName("alex");

        entityManager.persist(businessEntry);
        entityManager.flush();

        BusinessEntry businessEntry1 = businessRepository.findByName("alex");
        assertThat(businessEntry1.getName()).isEqualTo(businessEntry.getName());
    }
}

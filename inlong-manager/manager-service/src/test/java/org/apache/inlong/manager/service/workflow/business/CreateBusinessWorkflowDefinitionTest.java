package org.apache.inlong.manager.service.workflow.business;

import com.sun.tools.javac.util.Assert;
import javax.validation.constraints.AssertTrue;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class CreateBusinessWorkflowDefinitionTest extends BaseTest {

    @Autowired
    CreateBusinessWorkflowDefinition createBusinessWorkflowDefinition;

    @Test
    public void testDefineProcess() {
        Process process = createBusinessWorkflowDefinition.defineProcess();
        Assert.check("Business Resource Creation".equals(process.getType()));
        Assert.check(process.getTaskByName("createHiveTableTask") != null);
        Assert.check(process.getNameToTaskMap().size() == 6);
    }


}

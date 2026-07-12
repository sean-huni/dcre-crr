package za.co.fnb.dcre.crr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;

@SpringBootApplication
@org.springframework.context.annotation.Import(za.co.fnb.dcre.platform.persistence.JdbcConfig.class)
public class CrrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(CrrApplication.class, args);
    }
}

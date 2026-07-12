package za.co.fnb.dcre.crr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;

@SpringBootApplication
public class CrrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(CrrApplication.class, args);
    }
}

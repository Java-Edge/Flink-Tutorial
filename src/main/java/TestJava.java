import org.relaxng.datatype.DatatypeException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author JavaEdge
 *
 * @date 2019-07-16
 */
public class TestJava {

    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/DD HH:mm:ss");
        Date begin = sdf.parse("1900/01/01 00:00:00");

        System.out.println(begin);
        Date target = sdf.parse("1900/01/02 00:00:01");
        System.out.println(target);

        long mid = target.getTime() - begin.getTime();
        System.out.println(mid);
    }


}

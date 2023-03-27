import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestDelimiter {
    public static void main(String[] args) {
        String value = "32:\n" +
                "15,4,2022-08-05\n" +
                "12,3,2022-08-05\n" +
                "10,1,2022-08-05";
        Pattern pattern = Pattern.compile("^[0-9]+:$");
        Matcher matcher = pattern.matcher(value);
        if (matcher.find()) {

            Scanner s = new Scanner(value).useDelimiter(",");
            int x = s.nextInt();
            double y = s.nextDouble();

            System.out.println("value of x - " + x);
            System.out.println("value of y - " + y);
        }
    }
}

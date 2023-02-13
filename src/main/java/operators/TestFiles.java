package operators;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

public class TestFiles {

    public static void main(String[] args) throws Exception {


        for(int i=0;i<1;i++) {

            // Read the file line by line
            BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Nikos\\PycharmProjects\\pythonProject\\datasets\\sachs\\data_sachs"+i+"_5000")); // input file as args[0]
            int count = 0;
            String line = br.readLine();

            while (line != null) {

                count++;
                System.out.println(count + ")" + line);

                List<String> input = Arrays.asList(line.replaceAll(" ", "").split(","));
                if (input.size() != 11 && !line.equals("EOF")) {
                    System.out.println("Invalid input !!!");
                    for(char c : line.toCharArray()){ System.out.println("ASCII code : "+(int)c); }
                    System.out.println("File : "+"data_sachs"+i+"_5000");
                    return;
                }

                // Read the next line
                line = br.readLine();
            }

            System.out.println("File : "+"data_sachs"+i+"_5000");
            System.out.println("Total number of tuples : " + count);
            Thread.sleep(2000);
            br.close();
        }
    }
}

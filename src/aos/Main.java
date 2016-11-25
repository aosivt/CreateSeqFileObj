package aos;

import aos.Reseach.CreateSeqFromLargeTiff;

import java.io.IOException;

public class Main  {
    static String mainpath = "";
    public static void main(String[] args) {
	// write your code here
        try {


            mainpath = (args.length==0?"":args[0].toString());
            System.out.println("Заданый путь:" + (mainpath.equals("")?"Пуст":mainpath));
            new CreateSeqFromLargeTiff().funcCreateSeqFromTiff();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

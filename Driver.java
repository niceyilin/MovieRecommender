/**
 * Created by yilin on 11/16/16.
 */
public class Driver {

    public static void main(String [] args) throws Exception{

        //----------------MR1-------------------
        GroupByUser MR1 = new GroupByUser();

        String MR1input = args[0];
        String MR1output = "/MR1_output";
        String [] MR1args = {MR1input, MR1output};

        MR1.main(MR1args);

        //----------------MR2-------------------
        CoOccurance MR2 = new CoOccurance();

        String MR2input = MR1output;
        String MR2output = "/MR2_output";
        String [] MR2args = {MR2input, MR2output};

        MR2.main(MR2args);

        //----------------MR3-------------------
        Normalize MR3 = new Normalize();

        String MR3input = MR2output;
        String MR3output = "/MR3_output";
        String [] MR3args = {MR3input, MR3output};

        MR3.main(MR3args);

        //----------------MR4-------------------
        Multiplication MR4 = new Multiplication();

        String MR4input1 = MR3output;
        String MR4input2 = args[1];
        String MR4output = "/MR4_output";
        String [] MR4args = {MR4input1, MR4input2, MR4output};

        MR4.main(MR4args);

        //----------------MR5-------------------
        Sum MR5 = new Sum();

        String MR5input = MR4output;
        String MR5output = "/MR5_output";
        String [] MR5args = {MR5input, MR5output};

        MR5.main(MR5args);

        //----------------MR6-------------------
        Filter MR6 = new Filter();

        String MR6input1 = MR5output;
        String MR6input2 = MR1output;
        String MR6output = args[2];
        String [] MR6args = {MR6input1, MR6input2, MR6output, args[3]};

        MR6.main(MR6args);
    }

}

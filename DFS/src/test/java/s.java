import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class s {

    public static void main(String[] args) throws InterruptedException, IOException {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("1.1.1.1"), 110);
        System.out.println(address.getAddress().getHostAddress());
    }
}





















//        for (int i=0;i<100;i++){
//            for (int j=0;j<(i+"").length();j++)
//            System.out.print('\b');
//            System.out.print('█');
//            System.out.print(i);
//            Thread.sleep(500);
//
//        }
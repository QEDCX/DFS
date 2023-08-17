import java.io.IOException;
import java.net.Socket;
import java.util.Scanner;

public class c {
    public static void main(String[] args) throws IOException, InterruptedException {
        Socket socket = new Socket("localhost",3000);
//        Worker.checkConnect(socket,()->{
//            socket.connect(new InetSocketAddress());
//        });
        new Scanner(System.in).next();
        socket.getOutputStream().write(1);
    }
}

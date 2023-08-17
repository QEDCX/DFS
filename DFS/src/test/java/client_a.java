import client.ClientAPI;
import java.io.IOException;
public class client_a {
    public static void main(String[] args) throws IOException, InterruptedException {
        String resourceId = ClientAPI.StorageFile("localhost", 1025, "C:\\Users\\MSC\\Documents\\书籍\\TCPIP.pdf");
        System.out.println(resourceId);
    }
}
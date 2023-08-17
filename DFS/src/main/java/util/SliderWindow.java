package util;

import message.Ack;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class SliderWindow {
    private int size = 10;
    private DatagramSocket datagramSocket;
    class window{
        int seq;
        DatagramPacket datagramPacket;
        window(int seq, DatagramPacket datagramPacket) {
            this.seq = seq;
            this.datagramPacket = datagramPacket;
        }
    }
    List<window> windowList = new ArrayList<>();

    public SliderWindow(InetSocketAddress udpAddr) throws SocketException {
        this.datagramSocket = new DatagramSocket();
        datagramSocket.connect(udpAddr);
    }

    public DatagramSocket getDatagramSocket() {
        return datagramSocket;
    }

    public void addPacket(int seq, DatagramPacket packet){
        windowList.add(new window(seq,packet));
    }
    public boolean hasSpace(){return windowList.size() < size;}
    public boolean send() throws IOException, InterruptedException {
        System.out.println("窗口数量："+windowList.size()+"  |  窗口发送中... ");
        //估计一个确认消息占150字节
        datagramSocket.setReceiveBufferSize(windowList.size()*150);
        //发送缓冲区
        datagramSocket.setSendBufferSize(1024*1024);
        datagramSocket.setSoTimeout(50);

        //发送所有窗口
        for (window win:windowList){
            datagramSocket.send(win.datagramPacket);
        }
        //休眠5ms后接收确认消息，若超时未确认，则重发
        Thread.sleep(5);
        DatagramPacket rcvPacket = new DatagramPacket(new byte[200],200);
        //循环接收并检测未确认的数据包
        boolean isResend = false;           //重传
        double resendPercent=0;            //重传比例
        int count = windowList.size();     //初始窗口数量
        //接收确认包
        byte reSendCount = 0;
        while(windowList.size() > 0){
            try {
                datagramSocket.receive(rcvPacket);
                Ack ack = (Ack) Serialize.deserialize(rcvPacket.getData());
                System.out.println("rcv-ack: "+ack.ack);            //打印接收的确认号
                //删除确认过的数据包
                window win =null;
                for (window w:windowList){
                    if (w.seq == ack.ack) win= w;
                }
                if (win!=null){
                    windowList.remove(win);
                }
            }catch (SocketTimeoutException e){
                if (++reSendCount == 6) {
                    System.out.println("滑动窗口信息---已达到重传上限："+--reSendCount+" 次");
                    return false;
                }
                //超时重发
                isResend = true;
                resendPercent = windowList.size() * 1.0 / count;      //重发比例为剩余窗口除以初始窗口数量
                System.out.println("\n超时重传阶段...第 "+reSendCount+" 次\n未确认数: "+windowList.size());
                System.out.print("未确认的包序号：");
                //发送未确认的包
                for (window win:windowList){
                    System.out.print(win.seq+"  ");
                    datagramSocket.send(win.datagramPacket);
                }
                System.out.println();
            } catch (Exception e){System.out.println(e);}
        }   //while

        if (isResend) {
            size *= (1-resendPercent);
            if (size < 10) size = 10;
        }
        else {
            size *= 1.5;       //否则增加到1.5
            if (size > 80) size = 80;
        }
        System.out.println("窗口发送完毕\n");
        return true;
    }
}


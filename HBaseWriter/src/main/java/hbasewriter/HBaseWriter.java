package hbasewriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.DataInputStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

public class HBaseWriter
{
    // trieda pre jeden packet
    public static class Packet
    {
        public Packet() {}

        public String timestamp;
        public String protocol;
        public String sourceAddress;
        public String destinationAddress;
        public int sourcePort;
        public int destinationPort;
        public String data;
    }

    public static List<Packet> finalPackets = new ArrayList<Packet>();
    public static long id = 1;

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }

    public static void WriteToHBase()
    {
        System.out.println("Starting HBaseWriter!");
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));

        String tableName = "Agent";
        Connection connection = null;
        Table table = null;

        try
        {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
            System.out.println("Connection established!");

            for(Packet p : finalPackets)
            {
                Put newPacket = new Put(longToBytes(id));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("timestamp"), Bytes.toBytes(p.timestamp));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("protocol"), Bytes.toBytes(p.protocol));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("source_address"), Bytes.toBytes(p.sourceAddress));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("destination_address"), Bytes.toBytes(p.destinationAddress));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("source_port"), Bytes.toBytes(p.sourcePort));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("destination_port"), Bytes.toBytes(p.destinationPort));
                newPacket.addColumn(Bytes.toBytes("packet"), Bytes.toBytes("data"), Bytes.toBytes(p.data));
                table.put(newPacket);
                id++;
            }

            System.out.println("Data written: " + finalPackets.size());
        }
        catch(Exception e)
        {
            System.out.println("Connection error!");
        }
        finally
        {
            try
            {
                if(table != null)
                {
                   table.close();
                }

                if(connection != null && !connection.isClosed())
                {
                   connection.close();
                }
            }
            catch(Exception e2)
            {
                System.out.println("Error in connection closing!");
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println("Listening on port 9999!");
        ServerSocket serverSocket = new ServerSocket(9999);

        while(true)
        {
           try
           {
                Socket connectionSocket = serverSocket.accept();
                System.out.println("Connection established!");

                DataInputStream input = new DataInputStream(connectionSocket.getInputStream());
                String data = "";
                byte[] buffer = new byte[1024];
                

                // Receive 4 bytes in big endian order, convert them to a integer in little endian order
                // The agent will send that amount of data
                //
                // This was done because TCP does not guarantee to deliver all bytes at once. So
                // first we get # of bytes to receive & start reading them from the socket.
                byte[] len_bytes = new byte[4];
                if (input.read(len_bytes) != len_bytes.length)
                {
                    System.out.println("Didnt receive enough bytes for length of data");
                    continue;
                }

                ByteBuffer wrapped = ByteBuffer.wrap(len_bytes);
                wrapped.order(ByteOrder.LITTLE_ENDIAN);
                int data_length = wrapped.getInt();

                System.out.println("Preparing to receive " + data_length);

                boolean end = false;
                int bytes_read = 0;
                while (!end)
                {
                    bytes_read = input.read(buffer);
                    data += new String(buffer, 0, bytes_read);
                    if (data.length() == data_length)
                    {
                        end = true;
                    }
                }

                List<String> packets = Arrays.asList(data.split("\n"));

                int i = 0;
                while(true)
                {
                    if(packets.get(i).compareTo("End of Packets File") == 0)
                        break;

                    Packet packet = new Packet();
                    packet.timestamp = packets.get(i);
                    packet.protocol = packets.get(i + 1);
                    packet.sourceAddress = packets.get(i + 2);
                    packet.destinationAddress = packets.get(i + 3);
                    packet.sourcePort = Integer.parseInt(packets.get(i + 4));
                    packet.destinationPort = Integer.parseInt(packets.get(i + 5));

                    i += 6;
                    packet.data = "";

                    while(true)
                    {
                        if(i >= packets.size())
                            break;

                        String p = packets.get(i);

                        if(p.compareTo("End of packet") == 0)
                            break;

                        packet.data += p;
                        packet.data += " ";
                        i++;
                    }

                    i += 2;

                    finalPackets.add(packet);
                }

                System.out.println("Packets parsed: " + finalPackets.size());
                WriteToHBase();
                finalPackets.clear();
            }
            catch(Exception e)
            {
                    e.printStackTrace();
            }
        }
    }
}
                                                      

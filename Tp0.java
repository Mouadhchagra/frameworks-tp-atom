import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Tp0 {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "server";
        if (mode.equals("server")) {
            int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
            int cards = args.length > 2 ? Integer.parseInt(args[2]) : 3;
            new SimulationServer(port, cards).start();
        } else {
            String host = args.length > 1 ? args[1] : "localhost";
            int port = args.length > 2 ? Integer.parseInt(args[2]) : 5000;
            long filter = args.length > 3 ? Long.parseLong(args[3]) : -1;
            new MonitoringClient(host, port, new ConsoleAlertSystem(), filter).connectAndListen();
        }
    }

    static class Location {
        double latitude;
        double longitude;

        Location(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public String toString() {
            return latitude + ";" + longitude;
        }
    }

    static class Packet {
        long cardId;
        long timeFrame;
        double temperature;
        Location gps;
        String checksum;

        private Packet(Builder b) {
            this.cardId = b.cardId;
            this.timeFrame = b.timeFrame;
            this.temperature = b.temperature;
            this.gps = b.gps;
            long value = cardId ^ timeFrame ^ Double.doubleToLongBits(temperature);
            this.checksum = Long.toHexString(value).toUpperCase();
        }

        static class Builder {
            long cardId;
            long timeFrame;
            double temperature = -18.0;
            Location gps;

            Builder cardId(long cardId) {
                this.cardId = cardId;
                return this;
            }

            Builder timeFrame(long timeFrame) {
                this.timeFrame = timeFrame;
                return this;
            }

            Builder temperature(double temperature) {
                this.temperature = temperature;
                return this;
            }

            Builder gps(Location gps) {
                this.gps = gps;
                return this;
            }

            Packet build() {
                return new Packet(this);
            }
        }
    }

    static class CardReader {
        long id;

        CardReader(long id) {
            this.id = id;
        }

        long getId() {
            return id;
        }
    }

    static class PacketGenerator {
        CardReader reader;
        long currentTimeFrame = 0;

        PacketGenerator(CardReader reader) {
            this.reader = reader;
        }

        Packet generate() {
            return new Packet.Builder()
                    .cardId(reader.getId())
                    .timeFrame(currentTimeFrame++)
                    .temperature(readSimulatedTemp())
                    .gps(readSimulatedGps())
                    .build();
        }

        double readSimulatedTemp() {
            double t;
            if (Math.random() < 0.15) {
                t = -11.9 + Math.random() * 15.9;
            } else {
                t = -22.0 + Math.random() * 8.0;
            }
            return Math.round(t * 100.0) / 100.0;
        }

        Location readSimulatedGps() {
            double lat = 35.0 + Math.random() * 2.0;
            double lon = 9.0 + Math.random() * 2.0;
            return new Location(lat, lon);
        }
    }

    static class SimulationServer {
        int port;
        int cardCount;

        SimulationServer(int port, int cardCount) {
            this.port = port;
            this.cardCount = cardCount;
        }

        void start() throws IOException {
            ServerSocket server = new ServerSocket(port);
            System.out.println("Server listening on port " + port);
            while (true) {
                Socket socket = server.accept();
                System.out.println("Client connected: " + socket.getRemoteSocketAddress());
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
                for (int i = 0; i < cardCount; i++) {
                    PacketGenerator generator = new PacketGenerator(new CardReader(1000 + i));
                    CardSender sender = new CardSender(out, generator);
                    new Thread(sender).start();
                }
            }
        }
    }

    static class CardSender implements Runnable {
        DataOutputStream out;
        PacketGenerator generator;

        CardSender(DataOutputStream out, PacketGenerator generator) {
            this.out = out;
            this.generator = generator;
        }

        public void run() {
            try {
                while (true) {
                    Packet p = generator.generate();
                    synchronized (out) {
                        out.writeLong(p.cardId);
                        out.writeLong(p.timeFrame);
                        out.writeDouble(p.temperature);
                        out.writeDouble(p.gps.latitude);
                        out.writeDouble(p.gps.longitude);
                        out.writeUTF(p.checksum);
                        out.flush();
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                System.out.println("Sender stopped: " + e.getMessage());
            }
        }
    }

    interface PacketListener {
        void onPacketReceived(Packet p);

        void onTemperatureAlert(Packet p);

        void onConnectionError(String error);
    }

    static class ConsoleAlertSystem implements PacketListener {
        public void onPacketReceived(Packet p) {
            System.out.println("RX card=" + p.cardId + " frame=" + p.timeFrame
                    + " temp=" + p.temperature + "C gps=[" + p.gps + "]");
        }

        public void onTemperatureAlert(Packet p) {
            System.out.println("\u001B[31mWARNING cold chain broken: card=" + p.cardId
                    + " frame=" + p.timeFrame + " temp=" + p.temperature + "C (> -12C)\u001B[0m");
        }

        public void onConnectionError(String error) {
            System.out.println("\u001B[31mConnection error: " + error + "\u001B[0m");
        }
    }

    static class MonitoringClient {
        String host;
        int port;
        PacketListener listener;
        long filter;

        MonitoringClient(String host, int port, PacketListener listener, long filter) {
            this.host = host;
            this.port = port;
            this.listener = listener;
            this.filter = filter;
        }

        void connectAndListen() {
            try {
                Socket socket = new Socket(host, port);
                DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                while (true) {
                    long cardId = in.readLong();
                    long timeFrame = in.readLong();
                    double temperature = in.readDouble();
                    double lat = in.readDouble();
                    double lon = in.readDouble();
                    String checksum = in.readUTF();

                    Packet p = new Packet.Builder()
                            .cardId(cardId)
                            .timeFrame(timeFrame)
                            .temperature(temperature)
                            .gps(new Location(lat, lon))
                            .build();

                    if (filter != -1 && p.cardId != filter) {
                        continue;
                    }
                    listener.onPacketReceived(p);
                    if (p.temperature > -12.0) {
                        listener.onTemperatureAlert(p);
                    }
                }
            } catch (IOException e) {
                listener.onConnectionError(e.getMessage());
            }
        }
    }
}

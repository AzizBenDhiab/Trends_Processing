package streaming;

import java.io.*;
import java.net.*;
import java.util.Scanner;

public class SocketServer {
    public static void main(String[] args) {
        int port = 9999;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("🟢 Socket server started on port " + port);
            System.out.println("Waiting for Spark to connect...");

            // Attendre la connexion de Spark
            try (Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                System.out.println("✅ Spark connected!");
                System.out.println("You can now type article titles (type 'quit' to exit):");

                Scanner scanner = new Scanner(System.in);
                String input;

                while (!(input = scanner.nextLine()).equals("quit")) {
                    out.println(input);
                    System.out.println("📤 Sent: " + input);
                }

                System.out.println("Server stopping...");
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
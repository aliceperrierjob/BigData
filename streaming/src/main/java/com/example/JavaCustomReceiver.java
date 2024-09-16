package com.example;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.MalformedURLException;

public class JavaCustomReceiver extends Receiver<String> {

  String host = null;
  int port = -1;

  public JavaCustomReceiver() {
    super(StorageLevel.MEMORY_AND_DISK_2());

  }

  @Override
  public void onStart() {
    // Start the thread that receives data over a connection
    new Thread(() -> {
      try {
        receive();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }).start();
  }

  @Override
  public void onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private void receive() throws IOException {

    URL inputUrl;
    try {
      inputUrl = new URL("https://stream.wikimedia.org/v2/stream/recentchange");
      try (BufferedReader input = new BufferedReader(new InputStreamReader(inputUrl.openStream(), StandardCharsets.UTF_8))) {

        String datum = input.readLine();
        if (!":ok".equals(datum))
          throw new IOException("First message should be :ok while got " + datum);
  
        StringBuilder next = new StringBuilder();
  
        while ((datum = input.readLine()) != null) {
          if (datum.length() == 0) {
            // End of message
            if (next.length() > 0) {
              
              System.out.println("Wikipedia message: " + next.toString());
              next.setLength(0);
            }
          } else {
            if (next.length() > 0)
              next.append('\n');
            next.append(datum);
          }
        }
    } 
  }
    catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    
      /*
       * Socket socket = null;
       * String userInput = null;
       * 
       * try {
       * // connect to the server
       * socket = new Socket(host, port);
       * 
       * BufferedReader reader = new BufferedReader(
       * new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
       * 
       * // Until stopped or connection broken continue reading
       * while (!isStopped() && (userInput = reader.readLine()) != null) {
       * System.out.println("Received data '" + userInput + "'");
       * store(userInput);
       * }
       * reader.close();
       * socket.close();
       * 
       * // Restart in an attempt to connect again when server is active again
       * restart("Trying to connect again");
       * } catch(ConnectException ce) {
       * // restart if could not connect to server
       * restart("Could not connect", ce);
       * } catch(Throwable t) {
       * // restart if there is any other error
       * restart("Error receiving data", t);
       * }
       */
       
      
  }
}

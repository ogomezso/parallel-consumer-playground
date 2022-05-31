package org.github.ogomezso.playground.parallelconsumer.streams;

public class StreamsPcApp {
    
      public static void main(String[] args) {

            BasicPCStreamConsumer  streamConsumer = new BasicPCStreamConsumer();

            streamConsumer.createStream("input", "output", "temp", "app1");

            Runtime.getRuntime().addShutdownHook(new Thread(streamConsumer::close));
      }
   
}

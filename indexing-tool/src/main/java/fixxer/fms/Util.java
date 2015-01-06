package fixxer.fms;

import java.nio.ByteBuffer;

public final class Util {

  /**
   * Private default constructor prevents instantiation
   */
  private Util() {

  }

  /**
   * Pack passport series and number into 5 bytes
   */
  public static byte[] packPassport(int series, int number) {
    byte[] seriesBytes = ByteBuffer.allocate(4).putInt(series).array();
    byte[] numberBytes = ByteBuffer.allocate(4).putInt(number).array();
    return new byte[] {
      seriesBytes[2], seriesBytes[3],
      numberBytes[1], numberBytes[2], numberBytes[3]
    };
  }
}

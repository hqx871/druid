package org.apache.druid.benchmark;

import org.apache.datasketches.memory.Memory;

public final class Murmur3
{
  private static final int seed = 0;

  public static int hash32(Memory memory, int offset, int size)
  {
    int h1 = seed;
    final int len = size;
    while (size >= Integer.BYTES) {
      int k1 = memory.getInt(offset);
      k1 = mixK1(k1);
      h1 = mixH1(h1, k1);

      offset += Integer.BYTES;
      size -= Integer.BYTES;
    }
    int k1 = 0;
    switch (size) {
      case 3:
        k1 ^= memory.getByte(offset) << 16;
        offset++;
      case 2:
        k1 ^= memory.getByte(offset) << 8;
        offset++;
      case 1:
        k1 ^= memory.getByte(offset);
        k1 = mixK1(k1);
        h1 ^= k1;
      default:
    }

    h1 = fmix(h1, len);
    return h1;
  }

  private static int mixK1(int k1)
  {
    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;
    return c2 * Integer.rotateLeft(k1 * c1, 15);
    //return Groupers.smear(k1);
  }

  private static int mixH1(int h1, int k1)
  {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  private static int fmix(int h1, int length)
  {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}

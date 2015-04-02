package org.rarefiedredis;

import java.security.SecureRandom;
import java.math.BigInteger;

public final class RandomKey {
  private SecureRandom random = new SecureRandom();

  public String randkey() {
    return new BigInteger(130, random).toString(32);
  }
}

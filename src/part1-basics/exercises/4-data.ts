import assert from "node:assert";
import { Brand, Console, Data, Effect, Equal, Hash, HashSet, Schema } from "effect";

/**
 * # Exercise 1:
 *
 * Implement `equals` and `hash` for the Transaction class
 */

class Transaction implements Equal.Equal, Hash.Hash {
  public readonly id: string;
  public readonly time: Date;
  public readonly amount: number;

  constructor(id: string, amount: number, time: Date) {
    this.id = id;
    this.time = time;
    this.amount = amount;
  }

  [Equal.symbol](that: unknown) {
    return (that instanceof Transaction && that.id === this.id
      && that.amount === this.amount && that.time.getTime() === this.time.getTime());
  }

  [Hash.symbol]() {
    return Hash.number(this.time.getTime())
  }
}

assert(
  Equal.equals(
    new Transaction("1", 1, new Date(3)),
    new Transaction("1", 1, new Date(3))
  )
);

assert(
  Hash.hash(new Transaction("1", 1, new Date(3))) ===
  Hash.hash(new Transaction("1", 1, new Date(3)))
);

/**
 * # Exercise 2:
 *
 * Create a datatype for a string that has been guaranteed to be only ascii
 * Here is a regex for you to use : /^[\x00-\x7F]*$/
 */

type ASCIIString = string & Brand.Brand<"ASCIIString">

const make = Brand.refined<ASCIIString>(
  (_) => /^[\x00-\x7F]*$/.test(_),
  (_) => Brand.error('Expected ASCIIString')
)

function takesOnlyAscii(s: ASCIIString) {
  // ...
}

const string1: ASCIIString = make("hello");
const string2: ASCIIString = make("hello🌍");

takesOnlyAscii(string1);
takesOnlyAscii(string2);

package nl.architolk.xea2rdf;

import nl.architolk.xea2rdf.Convert;
import picocli.CommandLine;

public class Main {

  public static void main(String[] args) {
    new CommandLine(new Convert()).execute(args);
  }

}

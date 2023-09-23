package nl.architolk.xea2rdf;

import java.io.File;
import java.io.FileOutputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.lang.Runnable;
//import java.util.List;

@Command(name = "xea2rdf")
public class Convert implements Runnable{

  private static Connection conn = null;

  private static final Logger LOG = LoggerFactory.getLogger(Convert.class);

  @Option(names={"-i","-input"},description="Input file: <input.xea>")
  private String inputFile;
  @Option(names={"-o","-output"},description="Output file: <output.xml> or <output.ttl> or..")
  private String outputFile;

  @Override
  public void run() {
    if ((inputFile!=null) && (outputFile!=null)) {
      startConverting();
    } else {
      LOG.info("Usage: xea2rdf -i <input.xea> -o <output.xml>");
    }
  }

  private static String filterNote(String note) {
    if (note!=null) {
      byte[] winquotebytes = {-62,-110};
      String winquote = new String(winquotebytes);
      return note.replace("&#235;","ë").replace("&#233;","é").replace(winquote,"'");
    } else {
      return null;
    }
  }

  private void exportEATables() throws SQLException {
    System.out.println("@prefix ea: <http://www.sparxsystems.eu/def/ea#>.");
    System.out.println("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.");
    exportPackages();
    exportObjects();
    exportAttributes();
    exportConnectors();
    exportAttributeTags();
    exportConnectorTags();
    exportObjectProperties();
    exportXRefs();
  }

  private static void exportStringValue(String name, String value) {
    if (value!=null) {
      // Escape escape character, or turtle file will not have the correct syntax
      System.out.println("  " + name + " '''" + ((String)value).replaceAll("\\\\","\\\\\\\\") + "''';");
    }
  }

  private static void exportBooleanValue(String name, Boolean value) {
    if (value!=null) {
      System.out.println("  " + name + " " + value + ";");
    }
  }

  private static void exportGUID(String name, String guid) {
    if (guid!=null) {
      System.out.println("  " + name + " '" + guid.replaceAll("^\\{(.*)\\}$","$1") + "';");
    }
  }

  private static void exportObjectRef(String name, String type, String value) {
    if (value!=null) {
      System.out.println("  " + name + " <urn:" + type + ":" + value + ">;");
    }
  }

  private static void exportObjectDef(String type, String value) {
    System.out.println("<urn:" + type.toLowerCase() + ":" + value + "> a ea:" + type + ";");
  }

  private static void exportPackages() throws SQLException {
    //Note: all packages are objects of type "Package". Their guids will be the same!

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_package").executeQuery();
    while (rs.next()) {
      exportObjectDef("Package",rs.getString("Package_ID"));
      exportGUID("ea:guid",rs.getString("ea_guid"));
      exportStringValue("rdfs:label",rs.getString("Name"));
      if (rs.getInt("Parent_ID")!=0) {
        exportObjectRef("ea:parent","package",rs.getString("Parent_ID"));
      }
      exportStringValue("rdfs:comment",filterNote(rs.getString("Notes")));
      System.out.println(".");
    }
  }

  private static void exportObjects() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_object").executeQuery();
    while (rs.next()) {
      exportObjectDef("Object",rs.getString("Object_ID"));
      exportGUID("ea:guid",rs.getString("ea_guid"));
      exportStringValue("ea:type",rs.getString("Object_Type"));
      exportStringValue("ea:stereotype",rs.getString("Stereotype"));
      exportStringValue("rdfs:label",rs.getString("Name"));
      exportStringValue("ea:alias",rs.getString("Alias"));
      if ("0".equals(rs.getString("Abstract"))) {
        exportBooleanValue("ea:abstract",false);
      }
      if ("1".equals(rs.getString("Abstract"))) {
        exportBooleanValue("ea:abstract",true);
      }
      exportObjectRef("ea:package","package",rs.getString("Package_ID"));
      exportStringValue("rdfs:comment",filterNote(rs.getString("Note")));
      if (rs.getInt("Classifier")!=0) {
        exportObjectRef("ea:classifier","connector",rs.getString("Classifier")); //Used with ProxyConnector
      }
      System.out.println(".");
    }
  }

  private static void exportAttributes() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_attribute").executeQuery();
    while (rs.next()) {
      exportObjectDef("Attribute",rs.getString("ID"));
      exportGUID("ea:guid",rs.getString("ea_guid"));
      exportStringValue("rdfs:label",rs.getString("Name"));
      exportStringValue("ea:type",rs.getString("Type"));
      exportObjectRef("ea:classifier","object",rs.getString("Classifier")); //Reference to the object that represents the type
      exportStringValue("ea:stereotype",rs.getString("Stereotype"));
      exportObjectRef("ea:object","object",rs.getString("Object_ID"));
      exportStringValue("rdfs:comment",filterNote(rs.getString("Notes")));
      exportStringValue("ea:lowerBound",rs.getString("LowerBound")); //Propbabily correct as string?
      exportStringValue("ea:upperBound",rs.getString("UpperBound")); //Propbabily correct as string?
      System.out.println(".");
    }
  }

  private static void exportConnectors() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_connector").executeQuery();
    while (rs.next()) {
      exportObjectDef("Connector",rs.getString("Connector_ID"));
      exportGUID("ea:guid",rs.getString("ea_guid"));
      exportStringValue("ea:type",rs.getString("Connector_Type"));
      exportStringValue("ea:stereotype",rs.getString("Stereotype"));
      exportStringValue("rdfs:label",rs.getString("Name"));
      exportObjectRef("ea:start","object",rs.getString("Start_Object_ID"));
      exportObjectRef("ea:end","object",rs.getString("End_Object_ID"));
      exportObjectRef("ea:pdata1","object",rs.getString("PDATA1")); //Pretty obscure way to link an associationclass
      exportStringValue("ea:direction",rs.getString("Direction"));
      exportStringValue("ea:sourceRole",rs.getString("SourceRole"));
      exportStringValue("ea:destRole",rs.getString("DestRole"));
      exportStringValue("ea:sourceCard",rs.getString("SourceCard"));
      exportStringValue("ea:destCard",rs.getString("DestCard"));
      exportStringValue("ea:sourceIsNavigable",rs.getString("SourceIsNavigable")); //Unsure - maybe boolean? propably 0, 1 - not correct in original?
      exportStringValue("ea:destIsNavigable",rs.getString("DestIsNavigable")); //Unsure - maybe boolean?
      System.out.println(".");
    }
  }

  private static void exportAttributeTags() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_attributetag").executeQuery();
    while (rs.next()) {
      if (rs.getString("VALUE")!=null) { //MAYBE this doesn't work - we have to check...
        exportObjectDef("Attributetag",rs.getString("PropertyID"));
        exportGUID("ea:guid",rs.getString("ea_guid"));
        exportObjectRef("ea:element","attribute",rs.getString("ElementID"));
        exportStringValue("ea:property",rs.getString("Property"));
        exportStringValue("ea:value",rs.getString("VALUE"));
        exportStringValue("ea:notes",filterNote(rs.getString("NOTES")));
        System.out.println(".");
      }
    }
  }

  private static void exportConnectorTags() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_connectortag").executeQuery();
    while (rs.next()) {
      if (rs.getString("VALUE")!=null) {
        exportObjectDef("Connectortag",rs.getString("PropertyID"));
        exportGUID("ea:guid",rs.getString("ea_guid"));
        exportObjectRef("ea:element","connector",rs.getString("ElementID"));
        exportStringValue("ea:property",rs.getString("Property"));
        exportStringValue("ea:value",rs.getString("VALUE"));
        exportStringValue("ea:notes",filterNote(rs.getString("NOTES")));
        System.out.println(".");
      }
    }
  }

  private static void exportObjectProperties() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_objectproperties").executeQuery();
    while (rs.next()) {
      if (rs.getString("Value")!=null) {
        exportObjectDef("ObjectProperty",rs.getString("PropertyID"));
        exportGUID("ea:guid",rs.getString("ea_guid"));
        exportObjectRef("ea:element","object",rs.getString("Object_ID"));
        exportStringValue("ea:property",rs.getString("Property"));
        exportStringValue("ea:value",rs.getString("Value"));
        exportStringValue("ea:notes",filterNote(rs.getString("Notes")));
        System.out.println(".");
      }
    }
  }

  private static void exportXRefs() throws SQLException {

    ResultSet rs = conn.prepareStatement("SELECT * FROM t_xref").executeQuery();
    while (rs.next()) {
      if ("CustomProperties".equals(rs.getString("Name"))) {
        exportObjectDef("XRef",Integer.toString(rs.getRow()));
        exportGUID("ea:client",rs.getString("Client"));
        String description = rs.getString("Description");
        if (description!=null) {
          String[] params = description.split("@ENDPROP;");
          for (String param : params) {
            String name = param.replaceAll("^(.*)@NAME=(.*)@ENDNAME(.*)$","$2");
            String value = param.replaceAll("^(.*)@VALU=(.*)@ENDVALU(.*)$","$2");
            String type = param.replaceAll("^(.*)@TYPE=(.*)@ENDTYPE(.*)$","$2");
            if (!name.isEmpty()) {
              if (type.equals("Boolean")) {
                exportBooleanValue("ea:"+name,value.equals("1"));
              } else {
                exportStringValue("ea:"+name,value);
              }
            }
          }
        }
        System.out.println(".");
      }
      if ("Stereotypes".equals(rs.getString("Name"))) {
        exportObjectDef("XRef",Integer.toString(rs.getRow()));
        exportGUID("ea:client",rs.getString("Client"));
        String description = rs.getString("Description");
        if (description!=null) {
          String[] stereotypes = description.split("@ENDSTEREO;");
          for (String stereotype : stereotypes) {
            String name = stereotype.replaceAll("^(.*)@STEREO;Name=([^;]*);(.*)$","$2");
            if (!name.isEmpty()) {
              exportStringValue("ea:stereotype",name);
            }
          }
        }
        System.out.println(".");
      }
    }
  }

  private void printTables() throws SQLException {

    ResultSet rs = conn.getMetaData().getTables(null, null, null, null);
    while (rs.next()) {
      System.out.println(rs.getString("TABLE_NAME"));
    }

  }

  private void startConverting() {

    LOG.info("Starting conversion");
    try {
      // db parameters
      String url = "jdbc:sqlite:" + inputFile;
      // create a connection to the database
      conn = DriverManager.getConnection(url);

      System.out.println("Connection to SQLite has been established.");

      //printTables();
      exportEATables();

    } catch (SQLException e) {
        LOG.error(e.getMessage());
    } finally {
      try {
        if (conn != null) {
            conn.close();
        }
      } catch (SQLException ex) {
          LOG.error(ex.getMessage());
      }
    }
  }

}

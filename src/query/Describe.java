package query;

import global.Minibase;
import parser.AST_Describe;
import relop.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  private String fileName;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
    this.fileName = tree.getFileName();
    QueryCheck.tableExists(fileName);

  } // public Describe(AST_Describe tree) throws QueryException

  private void printSpace(int s){
    for (int i = 0; i < s; i++){
      System.out.print(" ");
    }
  }

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    Schema schema = Minibase.SystemCatalog.getSchema(fileName);
    schema.print();
    int type;
    int len;
    for (int i = 0; i < schema.getCount(); i++) {
      type = schema.fieldType(i);
      len = schema.fieldLength(i);
      switch (type) {

        case 11:
          System.out.print("INTEGER ");
          printSpace(len-7);
          break;

        case 12:
          System.out.print("FLOAT ");
          printSpace(len-5);
          break;

        case 13:
          System.out.print("STRING ");
          printSpace(len-6);
          break;

        case 21:
          System.out.print("COLNAME ");
          printSpace(len-7);
          break;

        case 22:
          System.out.print("FIELDNO ");
          printSpace(len-7);
          break;
      }
    }

    System.out.println("");
    // print the output message
//    System.out.println("(Not implemented)");

  } // public void execute()

} // class Describe implements Plan

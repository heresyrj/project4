package query;

import parser.AST_Update;
import relop.Predicate;
import relop.Schema;
import heap.HeapFile;
import relop.*;


/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

    String[] cols;
    String filename;
    Predicate[][] preds;
    Object[] values;
    int[] fieldno;
    Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {

      this.cols = tree.getColumns();
      this.filename = tree.getFileName();
      this.preds = tree.getPredicates();
      this.values = tree.getValues();

//      schema = Minibase.SystemCatalog.getSchema(filename);
      schema = QueryCheck.tableExists(filename);
      QueryCheck.predicates(schema, preds);
      fieldno = QueryCheck.updateFields(schema, cols);
      QueryCheck.updateValues(schema, fieldno, values);



  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
      HeapFile hf = new HeapFile(filename);
      FileScan scan = new FileScan(schema, hf);
      Tuple tuple;
      boolean flagOR, flagAND;
      int count = 0;
      while (scan.hasNext()){
          flagAND = true;
          tuple = scan.getNext();
          for (int i = 0; i < preds.length; i++){
              flagOR = false;
              for (int j = 0; j < preds[i].length; j++){
                  if (preds[i][j].evaluate(tuple)){
                      flagOR = true;
                  }
              }
              if (!flagOR){
                  flagAND = flagAND && flagOR;
                  break;
              }

          }
          if (flagAND){
              for (int i = 0; i < fieldno.length; i++){
                  tuple.setField(fieldno[i], values[i]);
              }
              count++;
          }
      }
      scan.close();

    // print the output message
    	System.out.println(count + " rows affected.");

  } // public void execute()

} // class Update implements Plan

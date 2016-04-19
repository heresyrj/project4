package query;

import global.Minibase;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import parser.AST_Delete;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

    private String fileName;
    private Schema schema;
    //( or or or ) and ( or or or )
    private Predicate[][] preds;

    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if table doesn't exist or predicates are invalid
     */
    public Delete(AST_Delete tree) throws QueryException {
        try {
            fileName = tree.getFileName();
            QueryCheck.tableExists(fileName);
            schema = Minibase.SystemCatalog.getSchema(fileName);
            preds = tree.getPredicates();
            QueryCheck.predicates(schema, preds);

        } catch (QueryException exc) {
            throw new QueryException(exc.getMessage());
        }
    } // public Delete(AST_Delete tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
        HeapFile hf = new HeapFile(fileName);
        FileScan scan = new FileScan(schema, hf);
        Tuple tuple;
        boolean flagOR, flagAND;
        int count = 0;
        RID rid;
        while (scan.hasNext()){
            flagAND = true;
            tuple = scan.getNext();
            rid = scan.getLastRID();
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
                hf.deleteRecord(rid);
                count++;
            }
        }

        scan.close();

        // print the output message
        System.out.println(count+" rows deleted.");

    } // public void execute()

} // class Delete implements Plan

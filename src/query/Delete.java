package query;

import global.Minibase;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import parser.AST_Delete;
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
    private boolean kleenestar = false;

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
            if(preds[0].length == 0) {
                //no preds specified
                kleenestar = true;
            }
        } catch (QueryException exc) {
            throw new QueryException(exc.getMessage());
        }
    } // public Delete(AST_Delete tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
        HeapFile hf = new HeapFile(fileName);

        /** 2 options. If there's hashindex built on the queried col, use the index
         * otherwise use heapscan */
        HeapScan scan = hf.openScan();

        Tuple t;
        RID rid = new RID();
        boolean eval = false;
        int counter = 0;
        /** if no predicates specified, delete all tuples (schema and hf will be left)
         * else delete tuples met the predicates*/
        if(kleenestar) {
            scan.getNext(rid);
            hf.deleteRecord(rid);
        } else {
            /** for each tuple */
            while(scan.hasNext()) {
                t = new Tuple(schema, scan.getNext(rid));
                /** check all pred[] evaled by AND relation */
                for (int i = 0; i< preds.length; i++) {
                    /** check all pred[][] evaled by OR relation */
                    for(int j = 0; j < preds[i].length; j++) {
                        /**if any predicate in preds[][] is true, the entire OR clause is true*/
                        if(preds[i][j].evaluate(t)) {
                            eval = true;
                            break;
                        }
                    }
                    /**if any OR predicate clause is false, the entire thing is false
                     * then break out the eval to check next tuple
                     * otherwise delete this tuple if preds met */
                    if(!eval) break;
                }

                if(eval) {
                    hf.deleteRecord(rid);
                    counter++;
                }
            }
        }

        // print the output message
        System.out.println(counter+" rows deleted.");

    } // public void execute()

} // class Delete implements Plan

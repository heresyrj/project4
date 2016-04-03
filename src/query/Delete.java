package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Delete;
import relop.Predicate;
import relop.Schema;

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

        if(kleenestar) {

        }


        // print the output message
        System.out.println("1 rows affected.");

    } // public void execute()

} // class Delete implements Plan

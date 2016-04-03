package query;

import global.Minibase;
import index.HashIndex;
import parser.AST_DropIndex;

/**
 * Execution plan for dropping indexes.
 */
class DropIndex implements Plan {
    protected String fileName;

    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if index doesn't exist
     */
    public DropIndex(AST_DropIndex tree) throws QueryException {

        // make sure the table exists
        fileName = tree.getFileName();
        try {
            fileName = tree.getFileName();
            QueryCheck.indexExists(fileName);
        } catch (QueryException exc) {
            throw new QueryException(exc.getMessage());
        }
        //


    } // public DropIndex(AST_DropIndex tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {

        // print the output message
        HashIndex index = new HashIndex(fileName);
        index.deleteFile();
        Minibase.SystemCatalog.dropIndex(fileName);
        System.out.println("Index dropped");

    } // public void execute()

} // class DropIndex implements Plan

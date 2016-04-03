package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

    private String fileName;
    private Object[] values;
    private Schema schema;
    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if table doesn't exists or values are invalid
     */
    public Insert(AST_Insert tree) throws QueryException {

        try {
            fileName = tree.getFileName();
            QueryCheck.tableExists(fileName);
            values = tree.getValues();
            schema = Minibase.SystemCatalog.getSchema(fileName);
            QueryCheck.insertValues(schema, values);
        } catch (QueryException exc) {
            throw new QueryException(exc.getMessage());
        }


    } // public Insert(AST_Insert tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute(){

        HeapFile hf = new HeapFile(fileName);
        Tuple tuple = new Tuple(schema);
        tuple.setAllFields(values);
        RID rid = tuple.insertIntoFile(hf);

        int fieldno;
        IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(fileName);
        for (IndexDesc ind : inds) {
            fieldno = schema.fieldNumber(ind.columnName);
            new HashIndex(ind.indexName).insertEntry(new SearchKey(values[fieldno]), rid);
        }

        // print the output message
        System.out.println("1 rows affected.");

    } // public void execute()

} // class Insert implements Plan

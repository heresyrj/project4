package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import parser.AST_CreateIndex;
import relop.FileScan;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

    protected String fileName;
    private String ixTable;
    private String ixColumn;

    /**
     * Optimizes the plan, given the parsed query.
     *
     * @throws QueryException if index already exists or table/column invalid
     */
    public CreateIndex(AST_CreateIndex tree) throws QueryException {
        // make sure the table exists
        try {
            fileName = tree.getFileName();
            QueryCheck.indexExists(fileName);
            ixTable = tree.getIxTable();
            ixColumn = tree.getIxColumn();
        } catch (QueryException exc) {
            throw new QueryException(exc.getMessage());
        }

    } // public CreateIndex(AST_CreateIndex tree) throws QueryException

    /**
     * Executes the plan and prints applicable output.
     */
    public void execute() {
        // 1
        //create the index
        HashIndex index = new HashIndex(fileName);
        // update to the catalog
        Minibase.SystemCatalog.createIndex(fileName, ixTable, ixColumn);
        // print the output message
        System.out.println("Index created.");

        // 2
        //get the table schema
        Schema schema = Minibase.SystemCatalog.getSchema(fileName);
        //it opens the HeapFile if fileName exists
        HeapFile hf = new HeapFile(fileName);
        FileScan scan = new FileScan(schema, hf);

        // 3
        //scan through to build up index
        Tuple t;
        Object key;
        RID rid;
        int counter = 0;
        while (scan.hasNext()) {
            counter++;
            t = scan.getNext();
            key = t.getField(ixColumn);
            rid = scan.getLastRID();
            index.insertEntry(new SearchKey(key), rid);
        }

        if (hf.getRecCnt() == counter) {
            System.out.println("indexes on " + counter + " rows created.");
        } else {
            System.out.println("mismatch on rec (createIndex)");
        }


    } // public void execute()

} // class CreateIndex implements Plan
